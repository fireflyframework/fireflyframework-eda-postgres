/*
 * Copyright 2024-2026 Firefly Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fireflyframework.eda.consumer.postgres;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import lombok.extern.slf4j.Slf4j;
import org.fireflyframework.eda.annotation.PublisherType;
import org.fireflyframework.eda.consumer.ConsumerHealth;
import org.fireflyframework.eda.consumer.EventConsumer;
import org.fireflyframework.eda.event.EventEnvelope;
import org.fireflyframework.eda.listener.EventListenerProcessor;
import org.fireflyframework.eda.properties.EdaProperties;
import org.fireflyframework.eda.publisher.postgres.PostgresChannelMapper;
import org.fireflyframework.eda.serialization.MessageSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * PostgreSQL implementation of {@link EventConsumer}.
 * <p>
 * The consumer dedicates a single long-lived R2DBC connection to receive
 * {@code NOTIFY} messages, and uses a pooled {@link ConnectionFactory} for
 * the {@code SELECT}/{@code UPDATE} statements that drain the outbox table.
 * Channels are derived from {@code @EventListener} annotations discovered by
 * {@link EventListenerProcessor}; if none are present, the channels listed
 * in {@code firefly.eda.consumer.postgres.default.channels} are used.
 * <p>
 * For each notification, the corresponding outbox row is fetched, an
 * {@link EventEnvelope} is built, the event is dispatched through the
 * listener processor, and the row is marked {@code PROCESSED} on success or
 * incremented (and possibly transitioned to {@code DEAD_LETTER}) on failure.
 * A periodic poll catches any rows that slipped past the live channel — for
 * example, when the listener was offline at insert time or the payload
 * exceeded the {@code NOTIFY} limit and the dispatcher had to fall back to a
 * scan.
 */
@Component
@ConditionalOnClass(ConnectionFactory.class)
@ConditionalOnBean(name = "fireflyEdaPostgresConsumerConnectionFactory")
@ConditionalOnProperty(prefix = "firefly.eda.consumer", name = "enabled", havingValue = "true")
@DependsOn("eventListenerProcessor")
@Slf4j
public class PostgresEventConsumer implements EventConsumer {

    private final ObjectProvider<ConnectionFactory> connectionFactoryProvider;
    private final EventListenerProcessor eventListenerProcessor;
    private final MessageSerializer messageSerializer;
    private final EdaProperties edaProperties;
    private final ObjectMapper objectMapper;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong messagesConsumed = new AtomicLong();
    private final AtomicLong messagesProcessed = new AtomicLong();
    private final AtomicLong messagesFailures = new AtomicLong();
    private final AtomicReference<PostgresqlConnection> listenConnection = new AtomicReference<>();
    private final AtomicReference<Disposable> notificationSubscription = new AtomicReference<>();
    private final AtomicReference<Disposable> pollingSubscription = new AtomicReference<>();
    private volatile Sinks.Many<EventEnvelope> eventSink = Sinks.many().multicast().onBackpressureBuffer();
    private final Set<String> subscribedChannels = Collections.synchronizedSet(new LinkedHashSet<>());

    public PostgresEventConsumer(
            @Qualifier("fireflyEdaPostgresConsumerConnectionFactory")
            ObjectProvider<ConnectionFactory> connectionFactoryProvider,
            EventListenerProcessor eventListenerProcessor,
            MessageSerializer messageSerializer,
            EdaProperties edaProperties,
            ObjectMapper objectMapper) {
        this.connectionFactoryProvider = connectionFactoryProvider;
        this.eventListenerProcessor = eventListenerProcessor;
        this.messageSerializer = messageSerializer;
        this.edaProperties = edaProperties;
        this.objectMapper = objectMapper;
        eventListenerProcessor.registerListenerChangeCallback(this::refreshChannels);
        log.info("Postgres event consumer initialised; channels will be (re)subscribed at start()");
    }

    @Override
    public Flux<EventEnvelope> consume() {
        return eventSink.asFlux()
                .doOnSubscribe(subscription -> {
                    if (!running.get()) {
                        start().subscribe();
                    }
                });
    }

    @Override
    public Flux<EventEnvelope> consume(String... destinations) {
        if (destinations == null || destinations.length == 0) {
            return consume();
        }
        Set<String> filter = new HashSet<>(Arrays.asList(destinations));
        return consume().filter(envelope -> envelope.destination() != null
                && filter.contains(envelope.destination()));
    }

    @Override
    public Mono<Void> start() {
        return Mono.defer(() -> {
            if (!running.compareAndSet(false, true)) {
                log.debug("Postgres event consumer already running");
                return Mono.empty();
            }
            eventSink = Sinks.many().multicast().onBackpressureBuffer();
            ConnectionFactory factory = connectionFactoryProvider.getIfAvailable();
            if (factory == null) {
                running.set(false);
                return Mono.error(new IllegalStateException(
                        "PostgreSQL EDA consumer ConnectionFactory is not available"));
            }
            return openListenConnection(factory)
                    .flatMap(this::subscribeToChannels)
                    .doOnSuccess(unused -> startPolling())
                    .doOnError(e -> {
                        log.error("Failed to start Postgres event consumer", e);
                        running.set(false);
                    });
        });
    }

    @Override
    public Mono<Void> stop() {
        return Mono.defer(() -> {
            if (!running.compareAndSet(true, false)) {
                return Mono.empty();
            }
            log.info("Stopping Postgres event consumer");
            disposeQuietly(notificationSubscription.getAndSet(null));
            disposeQuietly(pollingSubscription.getAndSet(null));
            eventSink.tryEmitComplete();
            PostgresqlConnection connection = listenConnection.getAndSet(null);
            if (connection == null) {
                return Mono.empty();
            }
            return Mono.from(connection.close())
                    .doOnError(e -> log.warn("Error closing Postgres LISTEN connection", e))
                    .onErrorResume(e -> Mono.empty())
                    .then();
        });
    }

    @PreDestroy
    public void shutdown() {
        stop().block(Duration.ofSeconds(5));
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public String getConsumerType() {
        return "POSTGRES";
    }

    @Override
    public boolean isAvailable() {
        return edaProperties.getConsumer().isEnabled()
                && connectionFactoryProvider.getIfAvailable() != null;
    }

    @Override
    public Mono<ConsumerHealth> getHealth() {
        Map<String, Object> details = new HashMap<>();
        details.put("running", isRunning());
        details.put("subscribed_channels", new LinkedHashSet<>(subscribedChannels));
        details.put("polling_interval", config().getPollingInterval().toString());
        details.put("outbox_table", qualifiedTable(config()));
        return Mono.just(ConsumerHealth.builder()
                .consumerType(getConsumerType())
                .available(isAvailable())
                .running(isRunning())
                .status(isAvailable() && isRunning() ? "UP" : "DOWN")
                .messagesConsumed(messagesConsumed.get())
                .messagesProcessed(messagesProcessed.get())
                .messagesFailures(messagesFailures.get())
                .lastChecked(Instant.now())
                .details(details)
                .build());
    }

    private Mono<PostgresqlConnection> openListenConnection(ConnectionFactory factory) {
        return Mono.from(factory.create())
                .flatMap(connection -> {
                    if (!(connection instanceof PostgresqlConnection pg)) {
                        return Mono.from(connection.close())
                                .then(Mono.error(new IllegalStateException(
                                        "Expected PostgresqlConnection for LISTEN, got "
                                                + connection.getClass().getName())));
                    }
                    listenConnection.set(pg);
                    Disposable subscription = pg.getNotifications()
                            .subscribeOn(Schedulers.boundedElastic())
                            .subscribe(
                                    notification -> handleNotification(notification.getName(),
                                            notification.getParameter()),
                                    error -> log.error("Postgres notification stream errored", error));
                    notificationSubscription.set(subscription);
                    return Mono.just(pg);
                });
    }

    private Mono<Void> subscribeToChannels(PostgresqlConnection connection) {
        Set<String> channels = resolveChannels();
        if (channels.isEmpty()) {
            log.warn("Postgres consumer has no channels configured; no events will be received");
            return Mono.empty();
        }
        return Flux.fromIterable(channels)
                .concatMap(channel -> Mono.from(connection.createStatement(
                                "LISTEN " + quoteIdent(channel)).execute())
                        .flatMap(result -> Mono.from(result.getRowsUpdated()))
                        .doOnSuccess(rows -> {
                            subscribedChannels.add(channel);
                            log.info("Postgres consumer LISTENing on channel '{}'", channel);
                        })
                        .then())
                .then();
    }

    private void startPolling() {
        Duration interval = config().getPollingInterval();
        if (interval == null || interval.isZero() || interval.isNegative()) {
            log.debug("Postgres consumer polling disabled (pollingInterval={})", interval);
            return;
        }
        Disposable subscription = Flux.interval(interval, interval, Schedulers.boundedElastic())
                .onBackpressureDrop()
                .concatMap(tick -> pollPendingEvents())
                .subscribe(
                        ignored -> { },
                        error -> log.warn("Postgres consumer polling failed; will retry on next tick",
                                error));
        pollingSubscription.set(subscription);
        log.info("Postgres consumer polling every {} as a NOTIFY fallback", interval);
    }

    private Mono<Void> pollPendingEvents() {
        EdaProperties.Consumer.PostgresConfig cfg = config();
        Set<String> channels = subscribedChannels;
        if (channels.isEmpty()) {
            return Mono.empty();
        }
        ConnectionFactory factory = connectionFactoryProvider.getIfAvailable();
        if (factory == null) {
            return Mono.empty();
        }
        String sql = "SELECT id FROM " + qualifiedTable(cfg)
                + " WHERE status = 'PENDING' AND attempts < $1 AND channel = ANY($2) "
                + "ORDER BY created_at LIMIT $3";
        String[] channelsArr = channels.toArray(new String[0]);
        int maxAttempts = cfg.getMaxAttempts();
        int batchSize = cfg.getBatchSize();
        return Mono.usingWhen(
                Mono.from(factory.create()),
                connection -> Flux.from(connection.createStatement(sql)
                                .bind("$1", maxAttempts)
                                .bind("$2", channelsArr)
                                .bind("$3", batchSize)
                                .execute())
                        .flatMap(result -> result.map((row, meta) -> row.get("id", Long.class)))
                        .concatMap(this::handleEventById)
                        .then(),
                Connection::close,
                (connection, throwable) -> Mono.from(connection.close()).then(Mono.error(throwable)),
                Connection::close);
    }

    private void handleNotification(String channel, String parameter) {
        log.debug("Postgres consumer received NOTIFY on channel '{}' with parameter '{}'",
                channel, parameter);
        Long id = parseLong(parameter);
        if (id == null) {
            log.warn("Skipping notification with non-numeric parameter '{}'", parameter);
            return;
        }
        handleEventById(id)
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                        ignored -> { },
                        error -> log.error("Failed to handle event id {} from channel '{}'", id, channel,
                                error));
    }

    private Mono<Void> handleEventById(Long id) {
        ConnectionFactory factory = connectionFactoryProvider.getIfAvailable();
        if (factory == null || id == null) {
            return Mono.empty();
        }
        EdaProperties.Consumer.PostgresConfig cfg = config();
        String table = qualifiedTable(cfg);
        String sql = "UPDATE " + table + " SET attempts = attempts + 1 "
                + "WHERE id = $1 AND status = 'PENDING' "
                + "RETURNING id, destination, channel, event_type, payload, headers, "
                + "transaction_id, publisher_type, connection_id, event_class, attempts";
        return Mono.usingWhen(
                Mono.from(factory.create()),
                connection -> Flux.from(connection.createStatement(sql).bind("$1", id).execute())
                        .flatMap(result -> result.map((row, meta) -> outboxRowFrom(row)))
                        .next()
                        .flatMap(this::dispatch),
                Connection::close,
                (connection, throwable) -> Mono.from(connection.close()).then(Mono.error(throwable)),
                Connection::close);
    }

    private OutboxRow outboxRowFrom(Row row) {
        Long rowId = row.get("id", Long.class);
        String destination = row.get("destination", String.class);
        String channel = row.get("channel", String.class);
        String eventType = row.get("event_type", String.class);
        ByteArrayHolder payload = readPayload(row);
        String headersJson = row.get("headers", String.class);
        String transactionId = row.get("transaction_id", String.class);
        String publisherType = row.get("publisher_type", String.class);
        String connectionId = row.get("connection_id", String.class);
        String eventClass = row.get("event_class", String.class);
        Integer attempts = row.get("attempts", Integer.class);
        return new OutboxRow(rowId, destination, channel, eventType, payload.bytes(), headersJson,
                transactionId, publisherType, connectionId, eventClass, attempts);
    }

    private ByteArrayHolder readPayload(Row row) {
        byte[] direct = row.get("payload", byte[].class);
        if (direct != null) {
            return new ByteArrayHolder(direct);
        }
        java.nio.ByteBuffer buffer = row.get("payload", java.nio.ByteBuffer.class);
        if (buffer != null) {
            byte[] dst = new byte[buffer.remaining()];
            buffer.get(dst);
            return new ByteArrayHolder(dst);
        }
        return new ByteArrayHolder(new byte[0]);
    }

    private Mono<Void> dispatch(OutboxRow row) {
        messagesConsumed.incrementAndGet();
        Map<String, Object> headers = deserializeHeaders(row.headersJson());
        headers.putIfAbsent("destination", row.destination());
        headers.putIfAbsent("event_class", row.eventClass());
        if (row.transactionId() != null) {
            headers.putIfAbsent("transaction_id", row.transactionId());
        }
        Object event = deserializeEvent(row.payload(), row.eventClass());
        EventEnvelope envelope = EventEnvelope.forConsuming(
                row.destination(),
                row.eventType() != null ? row.eventType() : eventTypeFrom(event),
                event,
                row.transactionId(),
                headers,
                EventEnvelope.EventMetadata.empty(),
                Instant.now(),
                getConsumerType(),
                row.connectionId() != null ? row.connectionId() : "default",
                new OutboxAckCallback(row.id()));
        emit(envelope);
        return eventListenerProcessor.processEvent(event, headers)
                .then(markProcessed(row.id()))
                .doOnSuccess(v -> messagesProcessed.incrementAndGet())
                .onErrorResume(error -> {
                    messagesFailures.incrementAndGet();
                    log.error("Listener pipeline rejected event id {}; recording failure", row.id(),
                            error);
                    return markFailed(row.id(), error);
                });
    }

    private void emit(EventEnvelope envelope) {
        Sinks.EmitResult result = eventSink.tryEmitNext(envelope);
        if (result.isFailure()) {
            log.debug("Postgres consumer dropped envelope for destination {} ({}): no active subscriber",
                    envelope.destination(), result);
        }
    }

    private Mono<Void> markProcessed(Long id) {
        return updateStatus(id, "UPDATE " + qualifiedTable(config())
                + " SET status = 'PROCESSED', processed_at = NOW(), error_message = NULL "
                + "WHERE id = $1");
    }

    private Mono<Void> markFailed(Long id, Throwable error) {
        EdaProperties.Consumer.PostgresConfig cfg = config();
        int maxAttempts = cfg.getMaxAttempts();
        String table = qualifiedTable(cfg);
        String sql = "UPDATE " + table + " SET status = CASE "
                + "WHEN attempts >= $2 THEN 'DEAD_LETTER' ELSE 'PENDING' END, "
                + "failed_at = NOW(), error_message = $3 WHERE id = $1";
        ConnectionFactory factory = connectionFactoryProvider.getIfAvailable();
        if (factory == null) {
            return Mono.empty();
        }
        String message = error != null && error.getMessage() != null
                ? error.getMessage() : "unknown error";
        if (message.length() > 4000) {
            message = message.substring(0, 4000);
        }
        String finalMessage = message;
        return Mono.usingWhen(
                Mono.from(factory.create()),
                connection -> Mono.from(connection.createStatement(sql)
                                .bind("$1", id)
                                .bind("$2", maxAttempts)
                                .bind("$3", finalMessage)
                                .execute())
                        .flatMap(result -> Mono.from(result.getRowsUpdated()))
                        .then(),
                Connection::close,
                (connection, throwable) -> Mono.from(connection.close()).then(Mono.error(throwable)),
                Connection::close);
    }

    private Mono<Void> updateStatus(Long id, String sql) {
        ConnectionFactory factory = connectionFactoryProvider.getIfAvailable();
        if (factory == null) {
            return Mono.empty();
        }
        return Mono.usingWhen(
                Mono.from(factory.create()),
                connection -> Mono.from(connection.createStatement(sql).bind("$1", id).execute())
                        .flatMap(result -> Mono.from(result.getRowsUpdated()))
                        .then(),
                Connection::close,
                (connection, throwable) -> Mono.from(connection.close()).then(Mono.error(throwable)),
                Connection::close);
    }

    private void refreshChannels() {
        if (!running.get()) {
            return;
        }
        PostgresqlConnection connection = listenConnection.get();
        if (connection == null) {
            return;
        }
        Set<String> desired = resolveChannels();
        Set<String> toListen = new LinkedHashSet<>(desired);
        toListen.removeAll(subscribedChannels);
        Set<String> toUnlisten = new LinkedHashSet<>(subscribedChannels);
        toUnlisten.removeAll(desired);

        if (toListen.isEmpty() && toUnlisten.isEmpty()) {
            return;
        }
        log.info("Refreshing Postgres LISTEN channels: +{} -{}", toListen, toUnlisten);
        Flux.fromIterable(toListen)
                .concatMap(channel -> Mono.from(connection.createStatement(
                                "LISTEN " + quoteIdent(channel)).execute())
                        .flatMap(result -> Mono.from(result.getRowsUpdated()))
                        .doOnSuccess(rows -> subscribedChannels.add(channel))
                        .then())
                .thenMany(Flux.fromIterable(toUnlisten)
                        .concatMap(channel -> Mono.from(connection.createStatement(
                                        "UNLISTEN " + quoteIdent(channel)).execute())
                                .flatMap(result -> Mono.from(result.getRowsUpdated()))
                                .doOnSuccess(rows -> subscribedChannels.remove(channel))
                                .then()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                        ignored -> { },
                        error -> log.error("Failed to refresh Postgres LISTEN channels", error));
    }

    private Set<String> resolveChannels() {
        Set<String> destinations = new LinkedHashSet<>(
                eventListenerProcessor.getTopicsForConsumerType("POSTGRES"));
        String configured = config().getChannels();
        if (configured != null && !configured.isBlank()) {
            for (String dest : configured.split(",")) {
                String trimmed = dest.trim();
                if (!trimmed.isEmpty()) {
                    destinations.add(trimmed);
                }
            }
        }
        Set<String> channels = new LinkedHashSet<>();
        for (String destination : destinations) {
            if ("*".equals(destination)) {
                continue;
            }
            channels.add(PostgresChannelMapper.toChannel(destination));
        }
        return channels;
    }

    private Object deserializeEvent(byte[] payload, String eventClassName) {
        if (payload == null || payload.length == 0) {
            return Collections.emptyMap();
        }
        if (eventClassName != null) {
            try {
                Class<?> clazz = Class.forName(eventClassName);
                return messageSerializer.deserialize(payload, clazz);
            } catch (ClassNotFoundException e) {
                log.warn("Event class '{}' not found, falling back to Object.class", eventClassName);
            } catch (Exception e) {
                log.warn("Failed to deserialize event as {}: {}", eventClassName, e.getMessage());
            }
        }
        try {
            return messageSerializer.deserialize(payload, Object.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize Postgres event payload", e);
        }
    }

    private Map<String, Object> deserializeHeaders(String headersJson) {
        if (headersJson == null || headersJson.isBlank()) {
            return new HashMap<>();
        }
        try {
            Map<String, Object> headers = objectMapper.readValue(headersJson,
                    new TypeReference<>() { });
            return headers != null ? new HashMap<>(headers) : new HashMap<>();
        } catch (Exception e) {
            log.warn("Failed to parse outbox headers JSON, using empty map: {}", e.getMessage());
            return new HashMap<>();
        }
    }

    private String eventTypeFrom(Object event) {
        return event != null ? event.getClass().getSimpleName() : "unknown";
    }

    private EdaProperties.Consumer.PostgresConfig config() {
        EdaProperties.Consumer.PostgresConfig cfg = edaProperties.getConsumer().getPostgres()
                .getOrDefault("default", null);
        return cfg != null ? cfg : new EdaProperties.Consumer.PostgresConfig();
    }

    private String qualifiedTable(EdaProperties.Consumer.PostgresConfig cfg) {
        String schema = cfg.getSchema() != null ? cfg.getSchema() : "public";
        String table = cfg.getOutboxTable() != null ? cfg.getOutboxTable() : "firefly_eda_outbox";
        return schema + "." + table;
    }

    private static String quoteIdent(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static Long parseLong(String value) {
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static void disposeQuietly(Disposable disposable) {
        if (disposable != null && !disposable.isDisposed()) {
            disposable.dispose();
        }
    }

    private record OutboxRow(Long id, String destination, String channel, String eventType,
                             byte[] payload, String headersJson, String transactionId,
                             String publisherType, String connectionId, String eventClass,
                             Integer attempts) { }

    private record ByteArrayHolder(byte[] bytes) { }

    /**
     * Acknowledgment callback that marks the outbox row as processed or failed.
     */
    private final class OutboxAckCallback implements EventEnvelope.AckCallback {
        private final Long id;

        OutboxAckCallback(Long id) {
            this.id = id;
        }

        @Override
        public Mono<Void> acknowledge() {
            return markProcessed(id);
        }

        @Override
        public Mono<Void> reject(Throwable error) {
            return markFailed(id, error);
        }
    }
}
