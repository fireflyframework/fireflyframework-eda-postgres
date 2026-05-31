# Firefly Framework - EDA PostgreSQL

[![CI](https://github.com/fireflyframework/fireflyframework-eda-postgres/actions/workflows/ci.yml/badge.svg)](https://github.com/fireflyframework/fireflyframework-eda-postgres/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.org)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.x-green.svg)](https://spring.io/projects/spring-boot)

> PostgreSQL transport adapter for the Firefly Framework EDA abstraction — reactive event delivery over a transactional outbox table with `LISTEN`/`NOTIFY` push and a polling fallback.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [How It Works](#how-it-works)
- [Configuration](#configuration)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Overview

`fireflyframework-eda-postgres` is the **PostgreSQL transport adapter** for the Firefly EDA abstraction. It plugs into the EDA core (`fireflyframework-eda`) and implements its publisher and consumer ports — `EventPublisher` and `EventConsumer` — on top of PostgreSQL, with **no broker to operate**: your application database *is* the event bus. Applications interact only with the framework's transport-agnostic API (`@PublishResult`, `@EventListener`, `EventPublisher`); this module turns those calls into outbox inserts and `LISTEN`/`NOTIFY`-driven dispatch.

PostgreSQL is one of several interchangeable transports in the EDA family. The active transport is selected per publish/listen via the `PublisherType` (resolution order `KAFKA → RABBITMQ → POSTGRES → APPLICATION_EVENT`, or `AUTO` to resolve from configuration). To route events through PostgreSQL you add this adapter to the classpath and enable it through `firefly.eda.publishers.postgres.*` / `firefly.eda.consumer.postgres.*`. Sibling transport adapters cover other brokers:

- **`fireflyframework-eda-kafka`** — Apache Kafka, via Spring Kafka.
- **`fireflyframework-eda-rabbitmq`** — RabbitMQ.
- **`fireflyframework-eda-postgres`** (this module) — PostgreSQL `LISTEN`/`NOTIFY` with a transactional outbox.

Publishing writes events into a transactional **outbox table** (`firefly_eda_outbox` by default) over R2DBC. An `AFTER INSERT` trigger fires `pg_notify(channel, id)` for each row, so any consumer holding a long-lived `LISTEN` connection on the matching channel receives the new event id and fetches the full payload from the outbox. A periodic **polling fallback** drains rows that slipped past the live channel — for example when no listener was attached at insert time, the connection dropped, or the payload exceeded PostgreSQL's `NOTIFY` size limit. Failed deliveries increment an attempt counter and, once `max-attempts` is exhausted, transition to `DEAD_LETTER` status instead of being lost.

The adapter is **driven exclusively by `firefly.eda.*` properties** and intentionally ignores Spring's native `spring.r2dbc.*` configuration, keeping a single, consistent configuration surface across every Firefly EDA transport. It owns its own dedicated connection factories so it never competes with your application's primary R2DBC pool. The publisher side is wired by `FireflyEdaPostgresPublisherAutoConfiguration`, the consumer side by `FireflyEdaPostgresConsumerAutoConfiguration`, and the `PostgresEventPublisher` / `PostgresEventConsumer` beans are discovered by the EDA core component scan.

## Features

- **Reactive `EventPublisher`** (`PostgresEventPublisher`) — non-blocking publishing that performs a single `INSERT` into the configured outbox table over R2DBC; the insert can participate in the caller's transaction so a publish failure rolls back the business write.
- **`LISTEN`/`NOTIFY`-driven `EventConsumer`** (`PostgresEventConsumer`) — a single long-lived R2DBC `LISTEN` connection receives `NOTIFY` signals for low-latency push, with short-lived query connections draining outbox rows; channels are derived automatically from discovered `@EventListener(consumerType = POSTGRES | AUTO)` annotations and refreshed live when listeners change.
- **Polling fallback** — a configurable periodic scan (`polling-interval`, default 30s) re-drains `PENDING` rows missed by `NOTIFY` (listener offline at insert time, dropped connection, oversized payload), so no event is silently lost. Set the interval to zero to disable.
- **Transactional outbox with retry & dead-lettering** — every event row tracks `status`, `attempts`, and `error_message`. Successful dispatch marks the row `PROCESSED`; failures increment `attempts` and, once `max-attempts` is reached, transition the row to `DEAD_LETTER` rather than retrying forever.
- **Deterministic `PostgresChannelMapper`** — converts arbitrary destinations (`order-events`, `user.created`) into valid PostgreSQL channel identifiers: sanitized, lowercased, `firefly_eda_`-prefixed, and capped at the 63-byte `NAMEDATALEN-1` limit with an 8-char SHA-256 suffix for disambiguation. Publisher and consumer compute the same channel name independently.
- **Automatic schema provisioning** — when `auto-create-schema` is `true`, the publisher auto-config idempotently creates the outbox table, a partial `(status, created_at)` index on `PENDING` rows, a `(channel, status)` index, the `firefly_eda_notify_event()` `pg_notify` function, and the `AFTER INSERT` trigger. Disable it to manage schema via Flyway/Liquibase.
- **Payloads beyond the NOTIFY limit** — because payloads live in the outbox row (only the row id travels over `NOTIFY`), events are not constrained by PostgreSQL's 8000-byte `NOTIFY` payload cap.
- **Dedicated, isolated connection factories** — a pooled publisher `ConnectionFactory` (`fireflyEdaPostgresPublisherConnectionFactory`) for inserts and a non-pooled consumer factory (`fireflyEdaPostgresConsumerConnectionFactory`) holding the long-lived `LISTEN` connection, both `@ConditionalOnMissingBean` so you can override them.
- **Acknowledgement semantics** — consumed envelopes carry an ack callback that marks the outbox row `PROCESSED` on `acknowledge()` or `FAILED`/`DEAD_LETTER` on `reject(...)`.
- **Health monitoring** — both sides expose `PublisherHealth` / `ConsumerHealth` (status, availability, outbox table, subscribed channels, consumed/processed/failed counters) for actuator-style introspection.
- **Type-aware deserialization** — uses the persisted `event_class` to reconstruct the original event type, with a graceful fallback to generic `Object` deserialization.
- **`AUTO` transport participation** — registers as the `POSTGRES` `PublisherType` so it is selectable both explicitly and through `AUTO` resolution.

## Requirements

- Java 21+ (Java 25 recommended)
- Spring Boot 3.x
- Maven 3.9+
- PostgreSQL 12+ (any server with `LISTEN`/`NOTIFY` support)
- `fireflyframework-eda` (the EDA core; pulled in transitively)

## Installation

```xml
<dependency>
    <groupId>org.fireflyframework</groupId>
    <artifactId>fireflyframework-eda-postgres</artifactId>
    <!-- Version is managed by the Firefly BOM / parent POM -->
</dependency>
```

The version is managed by the Firefly parent/BOM (`org.fireflyframework:fireflyframework-parent`), so you normally omit `<version>`. This adapter declares a dependency on the EDA core (`fireflyframework-eda`), so adding it pulls the ports, annotations, and `EdaProperties` in transitively.

## Quick Start

**1. Add the dependency** (see above). The PostgreSQL transport beans light up only when you enable them.

**2. Enable and configure the PostgreSQL transport** in `application.yml`:

```yaml
firefly:
  eda:
    publishers:
      enabled: true
      postgres:
        default:
          enabled: true
          host: localhost
          port: 5432
          database: firefly
          username: firefly
          password: firefly
          schema: public
          outbox-table: firefly_eda_outbox
          default-destination: events
          auto-create-schema: true   # creates the outbox table, indexes, NOTIFY function & trigger
    consumer:
      enabled: true
      postgres:
        default:
          enabled: true
          host: localhost
          port: 5432
          database: firefly
          username: firefly
          password: firefly
          channels: events            # used only when no @EventListener is discovered
          polling-interval: PT30S     # NOTIFY fallback; PT0S to disable
```

**3. Publish events** with the framework's transport-agnostic annotation — the method result is written to the outbox and dispatched via `NOTIFY`:

```java
@Service
public class PaymentService {

    @PublishResult(publisherType = PublisherType.POSTGRES,
                   destination = "payments",
                   eventType = "payment.completed")
    public Mono<PaymentCompleted> complete(PaymentCommand cmd) {
        return process(cmd); // returned value is inserted into the outbox for "payments"
    }
}
```

**4. Consume events** by annotating a handler — the consumer derives the channel automatically and `LISTEN`s for it:

```java
@Component
public class PaymentListener {

    @EventListener(destinations = "payments",
                   eventTypes = "payment.completed",
                   consumerType = PublisherType.POSTGRES)
    public Mono<Void> onPaymentCompleted(PaymentCompleted event) {
        log.info("Received payment: {}", event.id());
        return Mono.empty();
    }
}
```

No hand-written SQL, no `spring.r2dbc.*`, and no manual `LISTEN`/`NOTIFY` plumbing — the adapter owns the outbox schema, the dedicated connection factories, and the channel mapping.

## How It Works

```
@PublishResult ──▶ PostgresEventPublisher
                      │  INSERT INTO firefly_eda_outbox (... status='PENDING')
                      ▼
            ┌──────────────────────────┐
            │   firefly_eda_outbox      │  AFTER INSERT trigger
            │   (transactional outbox)  │  ──▶ pg_notify(channel, id)
            └──────────────────────────┘
                      │ NOTIFY (id only)            │ polling fallback
                      ▼                             ▼  (PENDING rows, attempts < max)
            PostgresEventConsumer ── LISTEN ── drains row, dispatches to @EventListener
                      │ success ──▶ status='PROCESSED'
                      │ failure ──▶ attempts++  (DEAD_LETTER once attempts ≥ max-attempts)
```

The outbox table (created automatically when `auto-create-schema: true`) carries the event `payload` (`BYTEA`), `headers` (`JSONB`), the resolved `channel`, `event_type`, `event_class`, and lifecycle columns (`status`, `attempts`, `error_message`, `created_at`, `processed_at`, `failed_at`). Only the row **id** travels over `NOTIFY`, so payloads are not bound by PostgreSQL's `NOTIFY` size limit and survive consumer restarts.

## Configuration

All configuration lives under `firefly.eda.*` and is bound from the core `EdaProperties`. Connections are keyed by an id (`default` shown); add more keys for multiple connections. The adapter deliberately ignores `spring.r2dbc.*`.

```yaml
firefly:
  eda:
    enabled: true                      # master EDA switch (default true)
    default-publisher-type: AUTO       # AUTO resolves POSTGRES from config when enabled

    publishers:
      enabled: false                   # global publisher switch (must be true to publish)
      postgres:
        default:
          enabled: false               # enable this PostgreSQL publisher connection
          host: localhost
          port: 5432
          database:                    # database name (no default)
          username:                    # database user (no default)
          password:                    # database password (no default)
          schema: public               # schema holding the outbox table
          outbox-table: firefly_eda_outbox
          default-destination: events  # used when publish() is called without a destination
          auto-create-schema: true     # create table/indexes/function/trigger at startup
          max-pool-size: 10            # publisher R2DBC pool size
          properties: {}               # extra R2DBC connection options

    consumer:
      enabled: false                   # global consumer switch (must be true to consume)
      postgres:
        default:
          enabled: false               # enable this PostgreSQL consumer connection
          host: localhost
          port: 5432
          database:
          username:
          password:
          schema: public
          outbox-table: firefly_eda_outbox
          channels: events             # comma-separated; used only when no @EventListener found
          polling-interval: PT30S      # NOTIFY fallback poll; PT0S disables polling
          max-attempts: 3              # attempts before a row goes to DEAD_LETTER
          batch-size: 50               # rows drained per poll cycle
          max-pool-size: 5             # query pool size (LISTEN uses a separate long-lived conn)
          properties: {}
```

### Publisher properties (`firefly.eda.publishers.postgres.<id>.*`)

| Property | Default | Description |
|---|---|---|
| `enabled` | `false` | Enables this PostgreSQL publisher connection. A bean is only created when this is `true`, `firefly.eda.publishers.enabled` is `true`, and `host` is non-empty. |
| `host` | `localhost` | PostgreSQL host. |
| `port` | `5432` | PostgreSQL port. |
| `database` / `username` / `password` | _(none)_ | Connection credentials. |
| `schema` | `public` | Schema that holds the outbox table. |
| `outbox-table` | `firefly_eda_outbox` | Outbox table name. |
| `default-destination` | `events` | Destination used when `publish()` is called without one. |
| `auto-create-schema` | `true` | When `true`, idempotently creates the outbox table, indexes, `firefly_eda_notify_event()` function, and `AFTER INSERT` trigger at startup. Set `false` to manage schema via Flyway/Liquibase. |
| `max-pool-size` | `10` | Maximum size of the publisher's R2DBC connection pool. |
| `properties` | `{}` | Additional R2DBC connection options. |

### Consumer properties (`firefly.eda.consumer.postgres.<id>.*`)

| Property | Default | Description |
|---|---|---|
| `enabled` | `false` | Enables this PostgreSQL consumer connection (requires `firefly.eda.consumer.enabled: true` and a non-empty `host`). |
| `host` / `port` | `localhost` / `5432` | PostgreSQL host and port. |
| `database` / `username` / `password` | _(none)_ | Connection credentials. |
| `schema` / `outbox-table` | `public` / `firefly_eda_outbox` | Where the outbox table lives. |
| `channels` | `events` | Comma-separated channels to `LISTEN` on **only when no `@EventListener` annotations are discovered**; discovered listeners take precedence. |
| `polling-interval` | `PT30S` | Periodic `NOTIFY` fallback poll for `PENDING` rows. Set to `PT0S` to disable polling and rely solely on `NOTIFY`. |
| `max-attempts` | `3` | Delivery attempts before a row is moved to `DEAD_LETTER`. |
| `batch-size` | `50` | Maximum rows drained per poll cycle. |
| `max-pool-size` | `5` | Query (SELECT/UPDATE) pool size. The `LISTEN` connection is long-lived and separate from this pool. |
| `properties` | `{}` | Additional R2DBC connection options. |

## Documentation

- Firefly Framework documentation hub and module catalog: [github.com/fireflyframework](https://github.com/fireflyframework)
- EDA core (ports, annotations, `EdaProperties`): [`fireflyframework-eda`](https://github.com/fireflyframework/fireflyframework-eda)
- Sibling transports: [`fireflyframework-eda-kafka`](https://github.com/fireflyframework/fireflyframework-eda-kafka), [`fireflyframework-eda-rabbitmq`](https://github.com/fireflyframework/fireflyframework-eda-rabbitmq)

## Contributing

Contributions are welcome. Please read the [CONTRIBUTING.md](CONTRIBUTING.md) guide for details on our code of conduct, development process, and how to submit pull requests.

## License

Copyright 2024-2026 Firefly Software Foundation.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
