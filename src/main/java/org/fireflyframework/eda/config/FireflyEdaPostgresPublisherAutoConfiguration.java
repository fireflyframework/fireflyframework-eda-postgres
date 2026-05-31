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

package org.fireflyframework.eda.config;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.fireflyframework.eda.properties.EdaProperties;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;

/**
 * Auto-configuration for the PostgreSQL EDA publisher infrastructure.
 * <p>
 * Creates a pooled {@link ConnectionFactory} dedicated to writing outbox rows
 * and, when {@code firefly.eda.publishers.postgres.default.auto-create-schema}
 * is {@code true}, provisions the outbox table, supporting index, NOTIFY
 * function, and AFTER INSERT trigger required by the
 * {@link org.fireflyframework.eda.publisher.postgres.PostgresEventPublisher}.
 * <p>
 * <strong>Configuration source:</strong>
 * {@code firefly.eda.publishers.postgres.default.*} -- never
 * {@code spring.r2dbc.*}.
 */
@AutoConfiguration(after = FireflyEdaAutoConfiguration.class)
@ConditionalOnClass({ConnectionFactory.class, PostgresqlConnectionFactory.class})
@ConditionalOnProperty(prefix = "firefly.eda", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(EdaProperties.class)
@Slf4j
public class FireflyEdaPostgresPublisherAutoConfiguration {

    public FireflyEdaPostgresPublisherAutoConfiguration(EdaProperties props) {
        if (props.getPublishers().isEnabled()) {
            var postgresPublisher = props.getPublishers().getPostgres().get("default");
            if (postgresPublisher != null && postgresPublisher.isEnabled()
                    && postgresPublisher.getHost() != null && !postgresPublisher.getHost().isEmpty()) {
                log.info("--------------------------------------------------------------------------------");
                log.info("FIREFLY EDA POSTGRES PUBLISHER - INITIALIZING");
                log.info("--------------------------------------------------------------------------------");
            } else {
                log.debug("Firefly EDA Postgres Publisher auto-configuration loaded but not creating beans (disabled or not configured)");
            }
        } else {
            log.debug("Firefly EDA Postgres Publisher auto-configuration loaded but not creating beans (publishers globally disabled)");
        }
    }

    /**
     * Creates the pooled R2DBC {@link ConnectionFactory} the publisher uses to
     * insert outbox rows. The bean is created only when the publisher is
     * explicitly enabled and a host is configured under
     * {@code firefly.eda.publishers.postgres.default.host}.
     */
    @Bean(name = "fireflyEdaPostgresPublisherConnectionFactory", destroyMethod = "dispose")
    @ConditionalOnMissingBean(name = "fireflyEdaPostgresPublisherConnectionFactory")
    @ConditionalOnExpression("${firefly.eda.publishers.enabled:false} && ${firefly.eda.publishers.postgres.default.enabled:false} && '${firefly.eda.publishers.postgres.default.host:}'.length() > 0")
    public ConnectionPool fireflyEdaPostgresPublisherConnectionFactory(EdaProperties props) {
        EdaProperties.Publishers.PostgresConfig cfg = props.getPublishers().getPostgres().get("default");
        log.info("Creating PostgreSQL EDA publisher ConnectionFactory: host={}:{}, database={}, schema={}, table={}",
                cfg.getHost(), cfg.getPort(), cfg.getDatabase(), cfg.getSchema(), cfg.getOutboxTable());

        PostgresqlConnectionConfiguration.Builder builder = PostgresqlConnectionConfiguration.builder()
                .host(cfg.getHost())
                .port(cfg.getPort());
        if (cfg.getDatabase() != null) {
            builder.database(cfg.getDatabase());
        }
        if (cfg.getUsername() != null) {
            builder.username(cfg.getUsername());
        }
        if (cfg.getPassword() != null) {
            builder.password(cfg.getPassword());
        }
        if (cfg.getSchema() != null) {
            builder.schema(cfg.getSchema());
        }
        if (cfg.getProperties() != null && !cfg.getProperties().isEmpty()) {
            java.util.Map<String, String> options = new java.util.HashMap<>();
            for (Map.Entry<String, Object> entry : cfg.getProperties().entrySet()) {
                if (entry.getValue() != null) {
                    options.put(entry.getKey(), entry.getValue().toString());
                }
            }
            if (!options.isEmpty()) {
                builder.options(options);
            }
        }
        PostgresqlConnectionFactory delegate = new PostgresqlConnectionFactory(builder.build());

        ConnectionPoolConfiguration poolConfig = ConnectionPoolConfiguration.builder(delegate)
                .maxSize(Math.max(1, cfg.getMaxPoolSize()))
                .initialSize(1)
                .maxIdleTime(Duration.ofMinutes(5))
                .build();
        ConnectionPool pool = new ConnectionPool(poolConfig);

        if (cfg.isAutoCreateSchema()) {
            initialiseSchema(pool, cfg);
        }

        log.info("PostgreSQL EDA publisher ConnectionFactory created successfully");
        log.info("--------------------------------------------------------------------------------");
        return pool;
    }

    /**
     * Provisions the outbox table, supporting index, NOTIFY function and
     * trigger. Statements are idempotent so repeated startups are safe.
     */
    private void initialiseSchema(ConnectionFactory factory,
                                  EdaProperties.Publishers.PostgresConfig cfg) {
        String schema = cfg.getSchema() != null ? cfg.getSchema() : "public";
        String table = cfg.getOutboxTable() != null ? cfg.getOutboxTable() : "firefly_eda_outbox";
        String qualified = "\"" + schema + "\".\"" + table + "\"";
        String triggerName = table + "_notify";
        String functionName = "\"" + schema + "\".firefly_eda_notify_event";

        String createTable = "CREATE TABLE IF NOT EXISTS " + qualified + " ("
                + "id BIGSERIAL PRIMARY KEY, "
                + "destination VARCHAR(255) NOT NULL, "
                + "channel VARCHAR(63) NOT NULL, "
                + "event_type VARCHAR(255), "
                + "payload BYTEA NOT NULL, "
                + "headers JSONB, "
                + "transaction_id VARCHAR(255), "
                + "publisher_type VARCHAR(50), "
                + "connection_id VARCHAR(50), "
                + "event_class VARCHAR(500), "
                + "status VARCHAR(20) NOT NULL DEFAULT 'PENDING', "
                + "attempts INT NOT NULL DEFAULT 0, "
                + "error_message TEXT, "
                + "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
                + "processed_at TIMESTAMPTZ, "
                + "failed_at TIMESTAMPTZ)";

        String createIndex = "CREATE INDEX IF NOT EXISTS " + table + "_status_created_idx "
                + "ON " + qualified + " (status, created_at) WHERE status = 'PENDING'";
        String createChannelIndex = "CREATE INDEX IF NOT EXISTS " + table + "_channel_idx "
                + "ON " + qualified + " (channel, status)";

        String createFunction = "CREATE OR REPLACE FUNCTION " + functionName + "() RETURNS TRIGGER AS $$ "
                + "BEGIN PERFORM pg_notify(NEW.channel, NEW.id::text); RETURN NEW; END; "
                + "$$ LANGUAGE plpgsql";

        String dropTrigger = "DROP TRIGGER IF EXISTS " + triggerName + " ON " + qualified;
        String createTrigger = "CREATE TRIGGER " + triggerName + " AFTER INSERT ON " + qualified
                + " FOR EACH ROW EXECUTE FUNCTION " + functionName + "()";

        try {
            Mono.usingWhen(
                            Mono.from(factory.create()),
                            connection -> executeAll(connection,
                                    createTable, createIndex, createChannelIndex,
                                    createFunction, dropTrigger, createTrigger),
                            Connection::close)
                    .block(Duration.ofSeconds(30));
            log.info("PostgreSQL EDA outbox schema ensured: {}.{}", schema, table);
        } catch (Exception e) {
            log.error("Failed to initialise PostgreSQL EDA outbox schema {}.{}: {}",
                    schema, table, e.getMessage());
            log.error("Publishing will fail until the schema is created manually");
        }
    }

    private Mono<Void> executeAll(Connection connection, String... statements) {
        Mono<Void> chain = Mono.empty();
        for (String sql : statements) {
            String stmt = sql;
            chain = chain.then(Mono.from(connection.createStatement(stmt).execute())
                    .flatMap(result -> Mono.from(result.getRowsUpdated()))
                    .then());
        }
        return chain;
    }
}
