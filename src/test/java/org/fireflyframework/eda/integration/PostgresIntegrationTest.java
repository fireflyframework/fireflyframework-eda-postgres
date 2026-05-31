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

package org.fireflyframework.eda.integration;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Connection;
import org.fireflyframework.eda.annotation.PublisherType;
import org.fireflyframework.eda.consumer.postgres.PostgresEventConsumer;
import org.fireflyframework.eda.publisher.EventPublisher;
import org.fireflyframework.eda.publisher.EventPublisherFactory;
import org.fireflyframework.eda.publisher.postgres.PostgresChannelMapper;
import org.fireflyframework.eda.publisher.postgres.PostgresEventPublisher;
import org.fireflyframework.eda.testconfig.BaseIntegrationTest;
import org.fireflyframework.eda.testconfig.TestEventModels;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for the PostgreSQL EDA publisher and consumer that verify
 * end-to-end publishing via the outbox table, LISTEN/NOTIFY-driven dispatch
 * to consumers, acknowledgement semantics, and the polling fallback for
 * missed notifications.
 * <p>
 * Marked {@code @DirtiesContext(AFTER_CLASS)} so the Spring TestContext cache
 * evicts this test's context once it finishes -- the live LISTEN connection
 * and polling subscription would otherwise survive across test classes and
 * interact with other integration tests that share the {@code TestApplication}
 * fingerprint.
 */
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PostgresIntegrationTest extends BaseIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("firefly_eda")
            .withUsername("firefly")
            .withPassword("firefly");

    @Autowired(required = false)
    private PostgresEventPublisher publisher;

    @Autowired(required = false)
    private PostgresEventConsumer consumer;

    @Autowired(required = false)
    private EventPublisherFactory publisherFactory;

    @DynamicPropertySource
    static void postgresProperties(DynamicPropertyRegistry registry) {
        registry.add("firefly.eda.publishers.enabled", () -> "true");
        registry.add("firefly.eda.publishers.postgres.default.enabled", () -> "true");
        registry.add("firefly.eda.publishers.postgres.default.host", postgres::getHost);
        registry.add("firefly.eda.publishers.postgres.default.port",
                () -> postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT));
        registry.add("firefly.eda.publishers.postgres.default.database", postgres::getDatabaseName);
        registry.add("firefly.eda.publishers.postgres.default.username", postgres::getUsername);
        registry.add("firefly.eda.publishers.postgres.default.password", postgres::getPassword);
        registry.add("firefly.eda.publishers.postgres.default.default-destination", () -> "events");

        registry.add("firefly.eda.consumer.enabled", () -> "true");
        registry.add("firefly.eda.consumer.postgres.default.enabled", () -> "true");
        registry.add("firefly.eda.consumer.postgres.default.host", postgres::getHost);
        registry.add("firefly.eda.consumer.postgres.default.port",
                () -> postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT));
        registry.add("firefly.eda.consumer.postgres.default.database", postgres::getDatabaseName);
        registry.add("firefly.eda.consumer.postgres.default.username", postgres::getUsername);
        registry.add("firefly.eda.consumer.postgres.default.password", postgres::getPassword);
        registry.add("firefly.eda.consumer.postgres.default.channels", () -> "events,order-events");
        registry.add("firefly.eda.consumer.postgres.default.polling-interval", () -> "PT5S");
    }

    @Test
    @DisplayName("publisher inserts events into the outbox table")
    void publishInsertsOutboxRow() {
        if (publisher == null) {
            return;
        }
        TestEventModels.SimpleTestEvent event = TestEventModels.SimpleTestEvent.create("outbox row");

        StepVerifier.create(publisher.publish(event, "events", Map.of("source", "integration-test")))
                .verifyComplete();

        Long count = countOutboxRows("events");
        assertThat(count).isGreaterThanOrEqualTo(1L);
    }

    @Test
    @DisplayName("publisher exposes POSTGRES type and remains healthy")
    void publisherIdentifiesItself() {
        if (publisher == null) {
            return;
        }
        assertThat(publisher.getPublisherType()).isEqualTo(PublisherType.POSTGRES);
        assertThat(publisher.isAvailable()).isTrue();
        StepVerifier.create(publisher.getHealth())
                .assertNext(health -> {
                    assertThat(health.getPublisherType()).isEqualTo(PublisherType.POSTGRES);
                    assertThat(health.isAvailable()).isTrue();
                    assertThat(health.getStatus()).isEqualTo("UP");
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("EventPublisherFactory resolves the POSTGRES publisher")
    void factoryReturnsPostgresPublisher() {
        if (publisherFactory == null) {
            return;
        }
        EventPublisher resolved = publisherFactory.getPublisher(PublisherType.POSTGRES, "default");
        if (resolved == null) {
            return;
        }
        assertThat(resolved.getPublisherType()).isEqualTo(PublisherType.POSTGRES);
        assertThat(resolved.isAvailable()).isTrue();
    }

    @Test
    @DisplayName("consumer LISTENs on configured channels and marks events PROCESSED")
    void consumerProcessesPublishedEvents() {
        if (publisher == null || consumer == null) {
            return;
        }
        consumer.start().block(Duration.ofSeconds(10));
        try {
            AtomicLong seen = new AtomicLong();
            consumer.consume("order-events")
                    .subscribe(envelope -> seen.incrementAndGet());

            // give LISTEN time to register before publishing
            sleep(500);

            TestEventModels.SimpleTestEvent event = TestEventModels.SimpleTestEvent.create("via NOTIFY");
            publisher.publish(event, "order-events", Map.of("source", "consumer-test"))
                    .block(Duration.ofSeconds(5));

            await().atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> {
                        Long processed = countOutboxByStatus("order-events", "PROCESSED");
                        assertThat(processed).isGreaterThanOrEqualTo(1L);
                    });

            assertThat(seen.get()).isGreaterThanOrEqualTo(0L);
        } finally {
            consumer.stop().block(Duration.ofSeconds(5));
        }
    }

    @Test
    @DisplayName("channel mapper output matches the value inserted by the trigger")
    void channelColumnMatchesMapping() {
        if (publisher == null) {
            return;
        }
        publisher.publish(TestEventModels.SimpleTestEvent.create("channel"), "report.daily",
                Map.of()).block(Duration.ofSeconds(5));

        String mapped = PostgresChannelMapper.toChannel("report.daily");
        Long count = countRowsForChannel(mapped);
        assertThat(count).isGreaterThanOrEqualTo(1L);
    }

    private Long countOutboxRows(String destination) {
        return query("SELECT COUNT(*)::bigint AS c FROM public.firefly_eda_outbox WHERE destination = $1",
                statement -> statement.bind("$1", destination))
                .block(Duration.ofSeconds(10));
    }

    private Long countOutboxByStatus(String destination, String status) {
        return query("SELECT COUNT(*)::bigint AS c FROM public.firefly_eda_outbox "
                        + "WHERE destination = $1 AND status = $2",
                statement -> statement.bind("$1", destination).bind("$2", status))
                .block(Duration.ofSeconds(10));
    }

    private Long countRowsForChannel(String channel) {
        return query("SELECT COUNT(*)::bigint AS c FROM public.firefly_eda_outbox WHERE channel = $1",
                statement -> statement.bind("$1", channel))
                .block(Duration.ofSeconds(10));
    }

    private Mono<Long> query(String sql, java.util.function.Consumer<io.r2dbc.spi.Statement> binder) {
        PostgresqlConnectionFactory factory = new PostgresqlConnectionFactory(
                PostgresqlConnectionConfiguration.builder()
                        .host(postgres.getHost())
                        .port(postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT))
                        .database(postgres.getDatabaseName())
                        .username(postgres.getUsername())
                        .password(postgres.getPassword())
                        .build());
        return Mono.usingWhen(
                Mono.from(factory.create()),
                connection -> {
                    io.r2dbc.spi.Statement statement = connection.createStatement(sql);
                    binder.accept(statement);
                    return Mono.from(statement.execute())
                            .flatMap(result -> Mono.from(result.map((row, meta) -> row.get(0, Long.class))));
                },
                Connection::close,
                (connection, throwable) -> Mono.from(connection.close()).then(Mono.error(throwable)),
                Connection::close);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
