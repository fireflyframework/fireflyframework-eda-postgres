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

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
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

import java.util.Map;

/**
 * Auto-configuration for the PostgreSQL EDA consumer infrastructure.
 * <p>
 * Produces a dedicated {@link PostgresqlConnectionFactory} the consumer uses
 * for the long-lived {@code LISTEN} connection as well as for short-lived
 * {@code SELECT}/{@code UPDATE} statements that drain the outbox table. The
 * factory is not wrapped in a pool: a single {@code LISTEN} connection is
 * kept open for the consumer's lifetime, while query connections are created
 * on demand and closed immediately after use.
 * <p>
 * <strong>Configuration source:</strong>
 * {@code firefly.eda.consumer.postgres.default.*} -- never
 * {@code spring.r2dbc.*}.
 */
@AutoConfiguration(after = FireflyEdaAutoConfiguration.class)
@ConditionalOnClass({ConnectionFactory.class, PostgresqlConnectionFactory.class})
@ConditionalOnProperty(prefix = "firefly.eda", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(EdaProperties.class)
@Slf4j
public class FireflyEdaPostgresConsumerAutoConfiguration {

    public FireflyEdaPostgresConsumerAutoConfiguration(EdaProperties props) {
        if (props.getConsumer().isEnabled()) {
            var postgresConsumer = props.getConsumer().getPostgres().get("default");
            if (postgresConsumer != null && postgresConsumer.isEnabled()
                    && postgresConsumer.getHost() != null && !postgresConsumer.getHost().isEmpty()) {
                log.info("--------------------------------------------------------------------------------");
                log.info("FIREFLY EDA POSTGRES CONSUMER - INITIALIZING");
                log.info("--------------------------------------------------------------------------------");
            } else {
                log.debug("Firefly EDA Postgres Consumer auto-configuration loaded but not creating beans (disabled or not configured)");
            }
        } else {
            log.debug("Firefly EDA Postgres Consumer auto-configuration loaded but not creating beans (consumers globally disabled)");
        }
    }

    @Bean(name = "fireflyEdaPostgresConsumerConnectionFactory")
    @ConditionalOnMissingBean(name = "fireflyEdaPostgresConsumerConnectionFactory")
    @ConditionalOnExpression("${firefly.eda.consumer.enabled:false} && ${firefly.eda.consumer.postgres.default.enabled:false} && '${firefly.eda.consumer.postgres.default.host:}'.length() > 0")
    public PostgresqlConnectionFactory fireflyEdaPostgresConsumerConnectionFactory(EdaProperties props) {
        EdaProperties.Consumer.PostgresConfig cfg = props.getConsumer().getPostgres().get("default");
        log.info("Creating PostgreSQL EDA consumer ConnectionFactory: host={}:{}, database={}, schema={}, table={}, channels={}, pollingInterval={}",
                cfg.getHost(), cfg.getPort(), cfg.getDatabase(), cfg.getSchema(), cfg.getOutboxTable(),
                cfg.getChannels(), cfg.getPollingInterval());

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
        PostgresqlConnectionFactory factory = new PostgresqlConnectionFactory(builder.build());
        log.info("PostgreSQL EDA consumer ConnectionFactory created successfully");
        log.info("--------------------------------------------------------------------------------");
        return factory;
    }
}
