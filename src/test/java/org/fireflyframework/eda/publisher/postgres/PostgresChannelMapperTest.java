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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PostgresChannelMapperTest {

    @Test
    @DisplayName("converts plain alphanumeric destination to prefixed lowercase channel")
    void plainDestination() {
        assertThat(PostgresChannelMapper.toChannel("orders")).isEqualTo("firefly_eda_orders");
    }

    @Test
    @DisplayName("replaces dots, dashes and slashes with underscores")
    void sanitizesSpecialCharacters() {
        assertThat(PostgresChannelMapper.toChannel("user.created")).isEqualTo("firefly_eda_user_created");
        assertThat(PostgresChannelMapper.toChannel("order-events")).isEqualTo("firefly_eda_order_events");
        assertThat(PostgresChannelMapper.toChannel("exchange/route.key"))
                .isEqualTo("firefly_eda_exchange_route_key");
    }

    @Test
    @DisplayName("lowercases uppercase characters")
    void lowercasesInput() {
        assertThat(PostgresChannelMapper.toChannel("OrderEvents")).isEqualTo("firefly_eda_orderevents");
    }

    @Test
    @DisplayName("uses a default channel for null and blank input")
    void defaultsForNullOrBlank() {
        assertThat(PostgresChannelMapper.toChannel(null)).isEqualTo("firefly_eda_default");
        assertThat(PostgresChannelMapper.toChannel("")).isEqualTo("firefly_eda_default");
        assertThat(PostgresChannelMapper.toChannel("   ")).isEqualTo("firefly_eda_default");
    }

    @Test
    @DisplayName("never exceeds PostgreSQL identifier limit of 63 bytes")
    void enforcesIdentifierLimit() {
        String veryLong = "a".repeat(120);
        String channel = PostgresChannelMapper.toChannel(veryLong);
        assertThat(channel).hasSizeLessThanOrEqualTo(63);
        assertThat(channel).startsWith("firefly_eda_");
        // The hashed suffix preserves uniqueness.
        assertThat(channel).matches("firefly_eda_a+_[0-9a-f]{8}");
    }

    @Test
    @DisplayName("produces deterministic output for the same input")
    void deterministicOutput() {
        String channel1 = PostgresChannelMapper.toChannel("user.profile.updated");
        String channel2 = PostgresChannelMapper.toChannel("user.profile.updated");
        assertThat(channel1).isEqualTo(channel2);
    }

    @Test
    @DisplayName("distinguishes destinations whose sanitised prefix collides")
    void disambiguatesLongCollisions() {
        String first = PostgresChannelMapper.toChannel("a".repeat(200) + "first");
        String second = PostgresChannelMapper.toChannel("a".repeat(200) + "second");
        assertThat(first).isNotEqualTo(second);
        assertThat(first).hasSizeLessThanOrEqualTo(63);
        assertThat(second).hasSizeLessThanOrEqualTo(63);
    }
}
