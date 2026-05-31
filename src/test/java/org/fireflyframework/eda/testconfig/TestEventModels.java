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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * Test event models for integration and unit tests.
 */
public class TestEventModels {

    /**
     * Simple test event with basic fields.
     */
    public static class SimpleTestEvent {
        private String id;
        private String message;
        private Instant timestamp;

        public SimpleTestEvent() {
        }

        public SimpleTestEvent(String id, String message, Instant timestamp) {
            this.id = id;
            this.message = message;
            this.timestamp = timestamp;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public Instant getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Instant timestamp) {
            this.timestamp = timestamp;
        }

        public static SimpleTestEvent create(String message) {
            return new SimpleTestEvent(UUID.randomUUID().toString(), message, Instant.now());
        }
    }

    /**
     * Order created event for testing business scenarios.
     */
    public static class OrderCreatedEvent {
        private String orderId;
        private String customerId;
        private Double amount;
        private String currency;
        private Instant createdAt;

        public OrderCreatedEvent() {
        }

        public OrderCreatedEvent(String orderId, String customerId, Double amount, String currency, Instant createdAt) {
            this.orderId = orderId;
            this.customerId = customerId;
            this.amount = amount;
            this.currency = currency;
            this.createdAt = createdAt;
        }

        public String getOrderId() {
            return orderId;
        }

        public void setOrderId(String orderId) {
            this.orderId = orderId;
        }

        public String getCustomerId() {
            return customerId;
        }

        public void setCustomerId(String customerId) {
            this.customerId = customerId;
        }

        public Double getAmount() {
            return amount;
        }

        public void setAmount(Double amount) {
            this.amount = amount;
        }

        public String getCurrency() {
            return currency;
        }

        public void setCurrency(String currency) {
            this.currency = currency;
        }

        public Instant getCreatedAt() {
            return createdAt;
        }

        public void setCreatedAt(Instant createdAt) {
            this.createdAt = createdAt;
        }

        public static OrderCreatedEvent create(String customerId, Double amount) {
            return new OrderCreatedEvent(UUID.randomUUID().toString(), customerId, amount, "USD", Instant.now());
        }
    }

    /**
     * User registered event for testing business scenarios.
     */
    public static class UserRegisteredEvent {
        private String userId;
        private String email;
        private String username;
        private Instant registeredAt;
        private String tenantId;
        private boolean premium;

        public UserRegisteredEvent() {
        }

        public UserRegisteredEvent(String userId, String email, String username, Instant registeredAt) {
            this.userId = userId;
            this.email = email;
            this.username = username;
            this.registeredAt = registeredAt;
        }

        public UserRegisteredEvent(String userId, String email, String username, Instant registeredAt, String tenantId, boolean premium) {
            this.userId = userId;
            this.email = email;
            this.username = username;
            this.registeredAt = registeredAt;
            this.tenantId = tenantId;
            this.premium = premium;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public Instant getRegisteredAt() {
            return registeredAt;
        }

        public void setRegisteredAt(Instant registeredAt) {
            this.registeredAt = registeredAt;
        }

        public String getTenantId() {
            return tenantId;
        }

        public void setTenantId(String tenantId) {
            this.tenantId = tenantId;
        }

        public boolean isPremium() {
            return premium;
        }

        public void setPremium(boolean premium) {
            this.premium = premium;
        }

        public static UserRegisteredEvent create(String email, String username) {
            return new UserRegisteredEvent(UUID.randomUUID().toString(), email, username, Instant.now());
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String userId;
            private String email;
            private String username;
            private Instant registeredAt;
            private String tenantId;
            private boolean premium;

            public Builder userId(String userId) {
                this.userId = userId;
                return this;
            }

            public Builder email(String email) {
                this.email = email;
                return this;
            }

            public Builder username(String username) {
                this.username = username;
                return this;
            }

            public Builder registeredAt(Instant registeredAt) {
                this.registeredAt = registeredAt;
                return this;
            }

            public Builder tenantId(String tenantId) {
                this.tenantId = tenantId;
                return this;
            }

            public Builder premium(boolean premium) {
                this.premium = premium;
                return this;
            }

            public UserRegisteredEvent build() {
                if (userId == null) {
                    userId = UUID.randomUUID().toString();
                }
                if (registeredAt == null) {
                    registeredAt = Instant.now();
                }
                return new UserRegisteredEvent(userId, email, username, registeredAt, tenantId, premium);
            }
        }
    }

    /**
     * Complex nested event for testing serialization.
     */
    public static class ComplexEvent {
        private String id;
        private NestedData data;
        private Instant timestamp;

        public ComplexEvent() {
        }

        public ComplexEvent(String id, NestedData data, Instant timestamp) {
            this.id = id;
            this.data = data;
            this.timestamp = timestamp;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public NestedData getData() {
            return data;
        }

        public void setData(NestedData data) {
            this.data = data;
        }

        public Instant getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Instant timestamp) {
            this.timestamp = timestamp;
        }

        public static class NestedData {
            private String field1;
            private Integer field2;
            private InnerData innerData;

            public NestedData() {
            }

            public NestedData(String field1, Integer field2, InnerData innerData) {
                this.field1 = field1;
                this.field2 = field2;
                this.innerData = innerData;
            }

            public String getField1() {
                return field1;
            }

            public void setField1(String field1) {
                this.field1 = field1;
            }

            public Integer getField2() {
                return field2;
            }

            public void setField2(Integer field2) {
                this.field2 = field2;
            }

            public InnerData getInnerData() {
                return innerData;
            }

            public void setInnerData(InnerData innerData) {
                this.innerData = innerData;
            }
        }

        public static class InnerData {
            private String value;
            private Boolean flag;

            public InnerData() {
            }

            public InnerData(String value, Boolean flag) {
                this.value = value;
                this.flag = flag;
            }

            public String getValue() {
                return value;
            }

            public void setValue(String value) {
                this.value = value;
            }

            public Boolean getFlag() {
                return flag;
            }

            public void setFlag(Boolean flag) {
                this.flag = flag;
            }
        }

        public static ComplexEvent create() {
            InnerData innerData = new InnerData("nested", true);
            NestedData nestedData = new NestedData("test", 42, innerData);
            return new ComplexEvent(UUID.randomUUID().toString(), nestedData, Instant.now());
        }
    }
}

