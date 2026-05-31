# Firefly Framework - EDA - PostgreSQL

[![CI](https://github.com/fireflyframework/fireflyframework-eda-postgres/actions/workflows/ci.yml/badge.svg)](https://github.com/fireflyframework/fireflyframework-eda-postgres/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.org)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.x-green.svg)](https://spring.io/projects/spring-boot)

> PostgreSQL transport adapter for the Firefly EDA abstraction, using a transactional outbox table with LISTEN/NOTIFY-driven dispatch and a polling fallback.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Overview

Firefly Framework EDA PostgreSQL implements the `EventPublisher` and `EventConsumer` abstractions from `fireflyframework-eda` on top of PostgreSQL. Publishing writes events into a transactional outbox table (`firefly_eda_outbox`) over R2DBC; an `AFTER INSERT` trigger issues a `pg_notify` so consumers receive events through a long-lived `LISTEN` connection, with a polling fallback that drains any rows missed while no listener was attached.

The module provides `PostgresEventPublisher` (outbox writer), `PostgresEventConsumer` (LISTEN/NOTIFY + polling drainer), and `PostgresChannelMapper`, a deterministic mapper that converts arbitrary destination strings into valid PostgreSQL channel identifiers (63-byte limit, SHA-256 disambiguation suffix).

Auto-configuration is provided via `FireflyEdaPostgresPublisherAutoConfiguration` and `FireflyEdaPostgresConsumerAutoConfiguration`, which create the dedicated R2DBC connection factories and, optionally, provision the outbox schema (table, indexes, NOTIFY function, and trigger). Configuration is bound from the core `EdaProperties` under `firefly.eda.publishers.postgres.*` and `firefly.eda.consumer.postgres.*`.

## Features

- `PostgresEventPublisher` writing events to a transactional outbox table over R2DBC
- `PostgresEventConsumer` using LISTEN/NOTIFY for low-latency dispatch with a polling fallback
- Deterministic `PostgresChannelMapper` for safe PostgreSQL channel names (63-byte limit, SHA-256 suffix)
- Automatic outbox schema provisioning (table, indexes, NOTIFY function, AFTER INSERT trigger)
- Acknowledgement semantics that mark outbox rows `PROCESSED`/`FAILED`
- Dedicated, pooled publisher connection factory and a single long-lived consumer LISTEN connection
- Participates in the EDA `AUTO` publisher selection as the `POSTGRES` transport
- Spring Boot auto-configuration via `FireflyEdaPostgresPublisherAutoConfiguration` and `FireflyEdaPostgresConsumerAutoConfiguration`
- Configurable via the core `EdaProperties` (`firefly.eda.publishers.postgres.*`, `firefly.eda.consumer.postgres.*`)

## Requirements

- Java 21+
- Spring Boot 3.x
- Maven 3.9+
- PostgreSQL 12+ (LISTEN/NOTIFY support)

## Installation

```xml
<dependency>
    <groupId>org.fireflyframework</groupId>
    <artifactId>fireflyframework-eda-postgres</artifactId>
    <version>26.05.07</version>
</dependency>
```

## Quick Start

```xml
<dependencies>
    <dependency>
        <groupId>org.fireflyframework</groupId>
        <artifactId>fireflyframework-eda</artifactId>
    </dependency>
    <dependency>
        <groupId>org.fireflyframework</groupId>
        <artifactId>fireflyframework-eda-postgres</artifactId>
    </dependency>
</dependencies>
```

## Configuration

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
          auto-create-schema: true
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
          channels: events,order-events
          polling-interval: PT30S
```

## Documentation

No additional documentation available for this project.

## Contributing

Contributions are welcome. Please read the [CONTRIBUTING.md](CONTRIBUTING.md) guide for details on our code of conduct, development process, and how to submit pull requests.

## License

Copyright 2024-2026 Firefly Software Foundation.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
