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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Maps arbitrary destination strings (e.g., {@code "order-events"},
 * {@code "user.created"}) to deterministic, valid PostgreSQL channel names
 * suitable for use with {@code LISTEN}/{@code NOTIFY}.
 * <p>
 * PostgreSQL identifiers are limited to 63 bytes (NAMEDATALEN - 1). Channel
 * names must be valid SQL identifiers when not quoted. To keep the mapping
 * deterministic across publisher and consumer:
 * <ul>
 *   <li>Non-alphanumeric characters are converted to {@code _}.</li>
 *   <li>The result is lowercased and prefixed with {@code firefly_eda_}.</li>
 *   <li>If the result exceeds 63 bytes, the tail is replaced with an 8-char
 *       lower-case hex SHA-256 digest of the original destination to keep the
 *       mapping unique.</li>
 * </ul>
 */
public final class PostgresChannelMapper {

    private static final String PREFIX = "firefly_eda_";
    private static final int MAX_CHANNEL_LENGTH = 63;

    private PostgresChannelMapper() {
        // utility
    }

    /**
     * Converts the given destination into a deterministic PostgreSQL channel name.
     *
     * @param destination the logical destination (topic/queue/exchange name)
     * @return a valid PostgreSQL channel name, never null
     */
    public static String toChannel(String destination) {
        if (destination == null || destination.isBlank()) {
            return PREFIX + "default";
        }

        StringBuilder sanitized = new StringBuilder(destination.length());
        for (int i = 0; i < destination.length(); i++) {
            char c = destination.charAt(i);
            if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
                sanitized.append(c);
            } else if (c >= 'A' && c <= 'Z') {
                sanitized.append(Character.toLowerCase(c));
            } else {
                sanitized.append('_');
            }
        }

        String candidate = PREFIX + sanitized;
        if (candidate.length() <= MAX_CHANNEL_LENGTH) {
            return candidate;
        }

        String suffix = "_" + shortHash(destination);
        int keep = MAX_CHANNEL_LENGTH - suffix.length();
        return candidate.substring(0, keep) + suffix;
    }

    private static String shortHash(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(8);
            for (int i = 0; i < 4; i++) {
                hex.append(String.format("%02x", bytes[i]));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            return Integer.toHexString(value.hashCode());
        }
    }
}
