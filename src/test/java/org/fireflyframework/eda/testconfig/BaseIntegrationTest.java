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

package org.fireflyframework.eda.testconfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;

/**
 * Base class for integration tests providing common test infrastructure.
 * <p>
 * This class provides:
 * <ul>
 *   <li>Spring Boot test context configuration</li>
 *   <li>Test profile activation</li>
 *   <li>Common setup and teardown hooks</li>
 *   <li>Test isolation utilities</li>
 * </ul>
 */
@SpringBootTest(classes = TestApplication.class)
@ActiveProfiles("test")
public abstract class BaseIntegrationTest {

    protected static final Duration TEST_TIMEOUT = Duration.ofSeconds(30);
    protected static final Duration SHORT_TIMEOUT = Duration.ofSeconds(5);

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        // Common test properties
        registry.add("firefly.eda.enabled", () -> "true");
        registry.add("firefly.eda.metrics-enabled", () -> "true");
        registry.add("firefly.eda.health-enabled", () -> "true");
        registry.add("spring.main.allow-bean-definition-overriding", () -> "true");
    }

    @BeforeEach
    void baseSetUp() {
        // Common setup for all integration tests
    }

    @AfterEach
    void baseTearDown() {
        // Common cleanup for all integration tests
    }

    /**
     * Waits for a condition to be true with timeout.
     *
     * @param condition the condition to check
     * @param timeout the maximum time to wait
     * @throws InterruptedException if interrupted while waiting
     */
    protected void waitForCondition(java.util.function.BooleanSupplier condition, Duration timeout) 
            throws InterruptedException {
        long endTime = System.currentTimeMillis() + timeout.toMillis();
        while (!condition.getAsBoolean() && System.currentTimeMillis() < endTime) {
            Thread.sleep(100);
        }
        if (!condition.getAsBoolean()) {
            throw new AssertionError("Condition not met within timeout: " + timeout);
        }
    }
}

