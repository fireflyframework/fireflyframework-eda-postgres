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

package org.fireflyframework.eda.publisher.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import lombok.extern.slf4j.Slf4j;
import org.fireflyframework.eda.annotation.PublisherType;
import org.fireflyframework.eda.properties.EdaProperties;
import org.fireflyframework.eda.publisher.ConnectionAwarePublisher;
import org.fireflyframework.eda.publisher.EventPublisher;
import org.fireflyframework.eda.publisher.PublisherHealth;
import org.fireflyframework.eda.serialization.MessageSerializer;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * PostgreSQL implementation of {@link EventPublisher} using the transactional
 * outbox pattern combined with {@code LISTEN}/{@code NOTIFY}.
 * <p>
 * Each {@link #publish(Object, String, Map)} call performs a single
 * {@code INSERT} into the configured outbox table. A database trigger fires
 * {@code pg_notify(channel, id)} for every inserted row so any consumer that
 * is {@code LISTEN}ing on the matching channel receives the new event id and
 * can fetch the full payload from the outbox row.
 * <p>
 * Benefits over raw {@code NOTIFY}:
 * <ul>
 *   <li>Payloads beyond PostgreSQL's 8000-byte NOTIFY limit are supported.</li>
 *   <li>Events survive consumer crashes (status remains {@code PENDING}
 *       until processed).</li>
 *   <li>Retry counts and dead-letter status are tracked per row.</li>
 *   <li>The {@code INSERT} can participate in the caller's transaction so a
 *       publish failure rolls back the business write.</li>
 * </ul>
 */
@Component
@ConditionalOnClass(ConnectionFactory.class)
@ConditionalOnBean(name = "fireflyEdaPostgresPublisherConnectionFactory")
@Slf4j
public class PostgresEventPublisher implements EventPublisher, ConnectionAwarePublisher {

    private final ObjectProvider<ConnectionFactory> connectionFactoryProvider;
    private final MessageSerializer messageSerializer;
    private final EdaProperties edaProperties;
    private final ObjectMapper objectMapper;
    private String connectionId = "default";

    public PostgresEventPublisher(
            @Qualifier("fireflyEdaPostgresPublisherConnectionFactory")
            ObjectProvider<ConnectionFactory> connectionFactoryProvider,
            MessageSerializer messageSerializer,
            EdaProperties edaProperties,
            ObjectMapper objectMapper) {
        this.connectionFactoryProvider = connectionFactoryProvider;
        this.messageSerializer = messageSerializer;
        this.edaProperties = edaProperties;
        this.objectMapper = objectMapper;
    }

    @Override
    public Mono<Void> publish(Object event, String destination, Map<String, Object> headers) {
        if (event == null) {
            return Mono.error(new IllegalArgumentException("Event payload cannot be null"));
        }

        ConnectionFactory connectionFactory = connectionFactoryProvider.getIfAvailable();
        if (connectionFactory == null) {
            return Mono.error(new IllegalStateException(
                    "PostgreSQL EDA ConnectionFactory is not available"));
        }

        String effectiveDestination = destination != null ? destination : getDefaultDestination();
        if (effectiveDestination == null || effectiveDestination.isBlank()) {
            return Mono.error(new IllegalArgumentException(
                    "No destination provided and no default destination configured"));
        }

        EdaProperties.Publishers.PostgresConfig config = config();
        String table = qualifiedTable(config);
        String channel = PostgresChannelMapper.toChannel(effectiveDestination);
        Map<String, Object> mergedHeaders = mergeHeaders(headers, event);
        String headersJson = headersToJson(mergedHeaders);
        byte[] payload;
        try {
            payload = messageSerializer.serialize(event);
        } catch (Exception e) {
            return Mono.error(new RuntimeException("Failed to serialize event", e));
        }

        String eventType = eventTypeFrom(headers, event);
        String transactionId = extractStringHeader(headers, "transaction_id");
        String eventClass = event.getClass().getName();

        String sql = "INSERT INTO " + table + " ("
                + "destination, channel, event_type, payload, headers, "
                + "transaction_id, publisher_type, connection_id, event_class, status, attempts, created_at"
                + ") VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, 'PENDING', 0, NOW())";

        return Mono.usingWhen(
                Mono.from(connectionFactory.create()),
                connection -> executeInsert(connection, sql, effectiveDestination, channel, eventType,
                        payload, headersJson, transactionId, eventClass)
                        .doOnSuccess(rows -> log.debug(
                                "Inserted outbox row for destination={}, channel={}, rows={}",
                                effectiveDestination, channel, rows))
                        .then(),
                Connection::close,
                (connection, throwable) -> Mono.from(connection.close()).then(Mono.error(throwable)),
                Connection::close)
                .onErrorMap(e -> {
                    log.error("Failed to publish event to PostgreSQL outbox table {}", table, e);
                    return new RuntimeException("Failed to publish event to PostgreSQL", e);
                });
    }

    private Mono<Long> executeInsert(Connection connection, String sql,
                                     String destination, String channel, String eventType,
                                     byte[] payload, String headersJson, String transactionId,
                                     String eventClass) {
        Statement statement = connection.createStatement(sql)
                .bind("$1", destination)
                .bind("$2", channel)
                .bind("$4", payload)
                .bind("$5", headersJson)
                .bind("$9", eventClass);
        bindOrNull(statement, "$3", eventType, String.class);
        bindOrNull(statement, "$6", transactionId, String.class);
        statement.bind("$7", PublisherType.POSTGRES.name());
        statement.bind("$8", connectionId != null ? connectionId : "default");

        Publisher<? extends Result> execute = statement.execute();
        return Mono.from(execute).flatMap(result -> Mono.from(result.getRowsUpdated()).map(this::asLong));
    }

    private long asLong(Object value) {
        if (value instanceof Number n) {
            return n.longValue();
        }
        return 0L;
    }

    private void bindOrNull(Statement statement, String name, Object value, Class<?> type) {
        if (value == null) {
            statement.bindNull(name, type);
        } else {
            statement.bind(name, value);
        }
    }

    @Override
    public PublisherType getPublisherType() {
        return PublisherType.POSTGRES;
    }

    @Override
    public boolean isAvailable() {
        return connectionFactoryProvider.getIfAvailable() != null;
    }

    @Override
    public String getDefaultDestination() {
        EdaProperties.Publishers.PostgresConfig config = config();
        return config != null ? config.getDefaultDestination() : "events";
    }

    @Override
    public Mono<PublisherHealth> getHealth() {
        ConnectionFactory factory = connectionFactoryProvider.getIfAvailable();
        PublisherHealth.PublisherHealthBuilder builder = PublisherHealth.builder()
                .publisherType(getPublisherType())
                .connectionId(getConnectionId())
                .lastChecked(Instant.now());

        if (factory == null) {
            return Mono.just(builder
                    .status("DOWN")
                    .available(false)
                    .errorMessage("PostgreSQL EDA ConnectionFactory is not available")
                    .build());
        }

        return Mono.usingWhen(
                        Mono.from(factory.create()),
                        connection -> Mono.from(connection.validate(io.r2dbc.spi.ValidationDepth.REMOTE)),
                        Connection::close)
                .map(valid -> {
                    if (Boolean.TRUE.equals(valid)) {
                        return builder
                                .status("UP")
                                .available(true)
                                .details(Map.of(
                                        "connection_id", getConnectionId(),
                                        "outbox_table", qualifiedTable(config())))
                                .build();
                    }
                    return builder
                            .status("DOWN")
                            .available(false)
                            .errorMessage("PostgreSQL connection failed validation")
                            .build();
                })
                .onErrorResume(e -> {
                    log.warn("PostgreSQL publisher health check failed for connection {}",
                            getConnectionId(), e);
                    return Mono.just(builder
                            .status("DOWN")
                            .available(false)
                            .errorMessage(e.getMessage())
                            .details(Map.of("error_type", e.getClass().getSimpleName()))
                            .build());
                });
    }

    @Override
    public void setConnectionId(String connectionId) {
        this.connectionId = connectionId != null ? connectionId : "default";
    }

    @Override
    public String getConnectionId() {
        return connectionId;
    }

    @Override
    public boolean isConnectionConfigured(String connectionId) {
        return connectionFactoryProvider.getIfAvailable() != null
                && edaProperties.getPublishers().getPostgres()
                .getOrDefault(connectionId != null ? connectionId : "default", null) != null;
    }

    private EdaProperties.Publishers.PostgresConfig config() {
        Object resolved = edaProperties.getPublisherConfig(PublisherType.POSTGRES, connectionId);
        return resolved instanceof EdaProperties.Publishers.PostgresConfig postgres ? postgres : null;
    }

    private String qualifiedTable(EdaProperties.Publishers.PostgresConfig config) {
        if (config == null) {
            return "public.firefly_eda_outbox";
        }
        String schema = config.getSchema() != null ? config.getSchema() : "public";
        String table = config.getOutboxTable() != null ? config.getOutboxTable() : "firefly_eda_outbox";
        return schema + "." + table;
    }

    private Map<String, Object> mergeHeaders(Map<String, Object> headers, Object event) {
        Map<String, Object> merged = new HashMap<>();
        if (headers != null) {
            headers.forEach((k, v) -> {
                if (v != null) {
                    merged.put(k, v);
                }
            });
        }
        merged.put("publisher_type", PublisherType.POSTGRES.name());
        merged.put("connection_id", connectionId);
        merged.put("event_class", event.getClass().getName());
        merged.put("published_at", Instant.now().toString());
        return merged;
    }

    private String headersToJson(Map<String, Object> headers) {
        try {
            return objectMapper.writeValueAsString(headers);
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize headers map; falling back to empty JSON object", e);
            return "{}";
        }
    }

    private String eventTypeFrom(Map<String, Object> headers, Object event) {
        if (headers != null) {
            Object explicit = headers.get("eventType");
            if (explicit == null) {
                explicit = headers.get("event_type");
            }
            if (explicit != null) {
                return explicit.toString();
            }
        }
        return event.getClass().getSimpleName();
    }

    private String extractStringHeader(Map<String, Object> headers, String key) {
        if (headers == null) {
            return null;
        }
        Object value = headers.get(key);
        return value != null ? value.toString() : null;
    }
}
