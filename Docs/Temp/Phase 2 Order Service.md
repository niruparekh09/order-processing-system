Phase 2 is the most important phase in the entire project. Everything we build here - the domain model, the Flyway migrations, the transactional outbox - is the foundation every other service depends on. We go step by step. Do not move to the next step until the current one is done.

---

## Phase 2 - Order Service Domain Layer & Outbox Pattern

---

### Step 1 - Open the Order Service in IntelliJ

Open IntelliJ IDEA. Go to **File → Open** and select the `order-service` folder specifically - not the root repo folder. IntelliJ should detect it as a Maven project and import it automatically.

Wait for the Maven sync to finish (bottom progress bar). You'll know it's done when the `src` folder tree is fully populated and there are no red underlines on the `pom.xml`.

---

### Step 2 - Configure `application.yml`

Spring Initializr generates `application.properties`. Delete it and create `application.yml` in its place at `src/main/resources/`.

```yaml
server:
  port: 8081

spring:
  application:
    name: order-service

  # ── DataSource ────────────────────────────────────────────────────────────
  datasource:
    url: jdbc:postgresql://localhost:5432/orders_db
    username: orders_user
    password: orders_pass
    driver-class-name: org.postgresql.Driver
    hikari:
      pool-name: OrderServiceHikariPool
      maximum-pool-size: 20
      minimum-idle: 5
      idle-timeout: 300000
      connection-timeout: 20000
      leak-detection-threshold: 60000

  # ── JPA ───────────────────────────────────────────────────────────────────
  jpa:
    hibernate:
      ddl-auto: validate # Flyway owns the schema, Hibernate only validates
    show-sql: false
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
        format_sql: true
        default_schema: public
        jdbc:
          batch_size: 25
        order_inserts: true
        order_updates: true

  # ── Flyway ────────────────────────────────────────────────────────────────
  flyway:
    enabled: true
    locations: classpath:db/migration
    baseline-on-migrate: false
    validate-on-migrate: true
    out-of-order: false

  # ── Kafka ─────────────────────────────────────────────────────────────────
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all # wait for all ISR replicas to acknowledge
      retries: 3
      properties:
        enable.idempotence: true # exactly-once producer semantics
        max.in.flight.requests.per.connection: 1
    consumer:
      group-id: order-service
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      auto-offset-reset: earliest
      enable-auto-commit: false # manual offset commit only

  # ── Redis ─────────────────────────────────────────────────────────────────
  data:
    redis:
      host: localhost
      port: 6379
      timeout: 2000ms
      lettuce:
        pool:
          max-active: 10
          max-idle: 5
          min-idle: 1

# ── Actuator ──────────────────────────────────────────────────────────────
management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics
  endpoint:
    health:
      show-details: always
  metrics:
    tags:
      application: ${spring.application.name}
  tracing:
    sampling:
      probability: 1.0

# ── Outbox Poller ─────────────────────────────────────────────────────────
outbox:
  poller:
    fixed-delay-ms: 1000
    batch-size: 10
    advisory-lock-id: 72857438 # arbitrary fixed long, unique per service
```

---

### Step 3 - Add Missing Dependencies to `pom.xml`

Open `pom.xml`. Spring Initializr does not include observability libraries. Add these inside the `<dependencies>` block:

```xml
<!-- ── Observability ─────────────────────────────────────────── -->
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>

<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-tracing-bridge-brave</artifactId>
</dependency>

<dependency>
    <groupId>io.zipkin.reporter2</groupId>
    <artifactId>zipkin-reporter-brave</artifactId>
</dependency>

<!-- ── JSON serialization for outbox payload ─────────────────── -->
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
</dependency>

<dependency>
    <groupId>com.fasterxml.jackson.datatype</groupId>
    <artifactId>jackson-datatype-jsr310</artifactId>
</dependency>

<!-- ── Testcontainers ────────────────────────────────────────── -->
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>junit-jupiter</artifactId>
    <scope>test</scope>
</dependency>

<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>postgresql</artifactId>
    <scope>test</scope>
</dependency>

<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>kafka</artifactId>
    <scope>test</scope>
</dependency>

<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka-test</artifactId>
    <scope>test</scope>
</dependency>
```

Also add the Testcontainers BOM inside `<dependencyManagement>`:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-bom</artifactId>
            <version>1.20.1</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

Run `./mvnw compile` after saving. Fix any issues before continuing.

---

### Step 4 - Create the Package Structure

Create these packages inside `src/main/java/com/orderprocessing/orderservice/`. Do this manually in IntelliJ by right-clicking the base package and selecting New → Package.

```
com.orderprocessing.orderservice
├── config/
├── controller/
├── domain/
│   ├── model/
│   └── repository/
├── dto/
│   ├── request/
│   └── response/
├── event/
├── exception/
├── outbox/
└── service/
```

This structure separates concerns cleanly. Every class you create will go into one of these packages. Do not create any classes yet - just the packages.

---

### Step 5 - Flyway Migration: Database Schema

Create the directory `src/main/resources/db/migration/`.

Inside it, create the file `V1__init_orders_schema.sql`. The filename format is exact and matters - Flyway parses it strictly.

```sql
-- ============================================================
-- V1: Initial schema for Order Service
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── orders ───────────────────────────────────────────────────────────────────
CREATE TABLE orders (
    id                  UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id         UUID            NOT NULL,
    idempotency_key     VARCHAR(255)    NOT NULL,
    status              VARCHAR(50)     NOT NULL DEFAULT 'PENDING',
    total_amount        NUMERIC(19, 4)  NOT NULL,
    currency            CHAR(3)         NOT NULL DEFAULT 'INR',
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    version             BIGINT          NOT NULL DEFAULT 0,

    CONSTRAINT chk_orders_status CHECK (
        status IN (
            'PENDING',
            'INVENTORY_RESERVED',
            'INVENTORY_FAILED',
            'COMPENSATING',
            'PAYMENT_FAILED',
            'COMPLETED',
            'CANCELLED'
        )
    ),
    CONSTRAINT chk_orders_amount   CHECK (total_amount > 0),
    CONSTRAINT uq_orders_idempotency_key UNIQUE (idempotency_key)
);

-- ── order_items ───────────────────────────────────────────────────────────────
CREATE TABLE order_items (
    id              UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id        UUID            NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id      UUID            NOT NULL,
    product_name    VARCHAR(500)    NOT NULL,
    quantity        INT             NOT NULL,
    unit_price      NUMERIC(19, 4)  NOT NULL,
    total_price     NUMERIC(19, 4)  NOT NULL,

    CONSTRAINT chk_order_items_quantity     CHECK (quantity > 0),
    CONSTRAINT chk_order_items_unit_price   CHECK (unit_price > 0),
    CONSTRAINT chk_order_items_total_price  CHECK (total_price > 0)
);

-- ── outbox_events ─────────────────────────────────────────────────────────────
CREATE TABLE outbox_events (
    id              UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),
    aggregate_id    UUID            NOT NULL,
    aggregate_type  VARCHAR(100)    NOT NULL,
    event_type      VARCHAR(100)    NOT NULL,
    event_version   VARCHAR(10)     NOT NULL DEFAULT 'v1',
    payload         JSONB           NOT NULL,
    status          VARCHAR(20)     NOT NULL DEFAULT 'PENDING',
    kafka_topic     VARCHAR(255)    NOT NULL,
    kafka_key       VARCHAR(255),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ,
    retry_count     INT             NOT NULL DEFAULT 0,
    last_error      TEXT,

    CONSTRAINT chk_outbox_status CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED'))
);

-- ── indexes ───────────────────────────────────────────────────────────────────

-- Outbox poller hot path - only scans PENDING rows
CREATE INDEX idx_outbox_pending
    ON outbox_events (created_at ASC)
    WHERE status = 'PENDING';

-- Order lookups by customer
CREATE INDEX idx_orders_customer_id
    ON orders (customer_id);

-- Order lookups by status (admin/support tooling)
CREATE INDEX idx_orders_status
    ON orders (status);

-- Outbox lookup by aggregate (debugging)
CREATE INDEX idx_outbox_aggregate_id
    ON outbox_events (aggregate_id);

-- ── auto-update updated_at trigger ───────────────────────────────────────────
CREATE OR REPLACE FUNCTION fn_update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION fn_update_updated_at();
```

---

### Step 6 - Domain Entities

Create each file exactly at the package path shown.

---

**`domain/model/OrderStatus.java`**

```java
package com.orderprocessing.orderservice.domain.model;

public enum OrderStatus {
    PENDING,
    INVENTORY_RESERVED,
    INVENTORY_FAILED,
    COMPENSATING,       // stock rollback in progress
    PAYMENT_FAILED,
    COMPLETED,
    CANCELLED
}
```

---

**`domain/model/Order.java`**

```java
package com.orderprocessing.orderservice.domain.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "orders")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "customer_id", nullable = false)
    private UUID customerId;

    @Column(name = "idempotency_key", nullable = false, unique = true)
    private String idempotencyKey;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    @Builder.Default
    private OrderStatus status = OrderStatus.PENDING;

    @Column(name = "total_amount", nullable = false, precision = 19, scale = 4)
    private BigDecimal totalAmount;

    @Column(name = "currency", nullable = false, length = 3)
    @Builder.Default
    private String currency = "INR";

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL,
               orphanRemoval = true, fetch = FetchType.LAZY)
    @Builder.Default
    private List<OrderItem> items = new ArrayList<>();

    // ── convenience method ───────────────────────────────────────────────────
    public void addItem(OrderItem item) {
        items.add(item);
        item.setOrder(this);
    }
}
```

---

**`domain/model/OrderItem.java`**

```java
package com.orderprocessing.orderservice.domain.model;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name = "order_items")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderItem {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "order_id", nullable = false)
    private Order order;

    @Column(name = "product_id", nullable = false)
    private UUID productId;

    @Column(name = "product_name", nullable = false)
    private String productName;

    @Column(name = "quantity", nullable = false)
    private Integer quantity;

    @Column(name = "unit_price", nullable = false, precision = 19, scale = 4)
    private BigDecimal unitPrice;

    @Column(name = "total_price", nullable = false, precision = 19, scale = 4)
    private BigDecimal totalPrice;
}
```

---

**`domain/model/OutboxEvent.java`**

```java
package com.orderprocessing.orderservice.domain.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "outbox_events")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "aggregate_id", nullable = false)
    private UUID aggregateId;

    @Column(name = "aggregate_type", nullable = false)
    private String aggregateType;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "event_version", nullable = false)
    @Builder.Default
    private String eventVersion = "v1";

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, columnDefinition = "jsonb")
    private String payload;              // serialized JSON string

    @Column(name = "status", nullable = false)
    @Builder.Default
    private String status = "PENDING";

    @Column(name = "kafka_topic", nullable = false)
    private String kafkaTopic;

    @Column(name = "kafka_key")
    private String kafkaKey;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "published_at")
    private OffsetDateTime publishedAt;

    @Column(name = "retry_count", nullable = false)
    @Builder.Default
    private Integer retryCount = 0;

    @Column(name = "last_error")
    private String lastError;
}
```

---

### Step 7 - Repositories

**`domain/repository/OrderRepository.java`**

```java
package com.orderprocessing.orderservice.domain.repository;

import com.orderprocessing.orderservice.domain.model.Order;
import com.orderprocessing.orderservice.domain.model.OrderStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface OrderRepository extends JpaRepository<Order, UUID> {

    Optional<Order> findByIdempotencyKey(String idempotencyKey);

    @Modifying
    @Query("UPDATE Order o SET o.status = :status WHERE o.id = :id")
    int updateStatus(@Param("id") UUID id, @Param("status") OrderStatus status);
}
```

---

**`domain/repository/OutboxEventRepository.java`**

```java
package com.orderprocessing.orderservice.domain.repository;

import com.orderprocessing.orderservice.domain.model.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    @Query(value = """
            SELECT * FROM outbox_events
            WHERE status = 'PENDING'
            ORDER BY created_at ASC
            LIMIT :batchSize
            """, nativeQuery = true)
    List<OutboxEvent> findPendingEvents(@Param("batchSize") int batchSize);
}
```

---

### Step 8 - DTOs

**`dto/request/OrderItemRequest.java`**

```java
package com.orderprocessing.orderservice.dto.request;

import jakarta.validation.constraints.*;
import lombok.Data;

import java.math.BigDecimal;
import java.util.UUID;

@Data
public class OrderItemRequest {

    @NotNull(message = "productId is required")
    private UUID productId;

    @NotBlank(message = "productName is required")
    private String productName;

    @NotNull
    @Min(value = 1, message = "quantity must be at least 1")
    private Integer quantity;

    @NotNull
    @DecimalMin(value = "0.01", message = "unitPrice must be greater than 0")
    private BigDecimal unitPrice;
}
```

---

**`dto/request/CreateOrderRequest.java`**

```java
package com.orderprocessing.orderservice.dto.request;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import lombok.Data;

import java.util.List;
import java.util.UUID;

@Data
public class CreateOrderRequest {

    @NotNull(message = "customerId is required")
    private UUID customerId;

    @NotEmpty(message = "Order must contain at least one item")
    @Valid
    private List<OrderItemRequest> items;

    @NotBlank(message = "currency is required")
    @Size(min = 3, max = 3, message = "currency must be a 3-letter ISO code")
    private String currency = "INR";
}
```

---

**`dto/response/OrderResponse.java`**

```java
package com.orderprocessing.orderservice.dto.response;

import com.orderprocessing.orderservice.domain.model.OrderStatus;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Data
@Builder
public class OrderResponse {
    private UUID orderId;
    private UUID customerId;
    private OrderStatus status;
    private BigDecimal totalAmount;
    private String currency;
    private List<OrderItemResponse> items;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;

    @Data
    @Builder
    public static class OrderItemResponse {
        private UUID productId;
        private String productName;
        private Integer quantity;
        private BigDecimal unitPrice;
        private BigDecimal totalPrice;
    }
}
```

---

### Step 9 - Event Envelope

This is the standard JSON structure every Kafka message in the system uses.

**`event/EventEnvelope.java`**

```java
package com.orderprocessing.orderservice.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Builder;
import lombok.Data;

import java.time.OffsetDateTime;
import java.util.UUID;

@Data
@Builder
public class EventEnvelope {

    private UUID eventId;
    private String eventType;
    private String eventVersion;
    private UUID aggregateId;
    private String aggregateType;
    private UUID correlationId;
    private UUID causationId;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    private OffsetDateTime occurredAt;

    private String producer;
    private Object payload;
}
```

---

**`event/OrderCreatedPayload.java`**

```java
package com.orderprocessing.orderservice.event;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@Data
@Builder
public class OrderCreatedPayload {

    private UUID orderId;
    private UUID customerId;
    private BigDecimal totalAmount;
    private String currency;
    private List<OrderItem> items;

    @Data
    @Builder
    public static class OrderItem {
        private UUID productId;
        private String productName;
        private Integer quantity;
        private BigDecimal unitPrice;
        private BigDecimal totalPrice;
    }
}
```

---

### Step 10 - Exception Classes

**`exception/OrderNotFoundException.java`**

```java
package com.orderprocessing.orderservice.exception;

import java.util.UUID;

public class OrderNotFoundException extends RuntimeException {
    public OrderNotFoundException(UUID orderId) {
        super("Order not found: " + orderId);
    }
}
```

---

**`exception/DuplicateOrderException.java`**

```java
package com.orderprocessing.orderservice.exception;

public class DuplicateOrderException extends RuntimeException {
    public DuplicateOrderException(String idempotencyKey) {
        super("Order already exists for idempotency key: " + idempotencyKey);
    }
}
```

---

**`exception/GlobalExceptionHandler.java`**

```java
package com.orderprocessing.orderservice.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.time.OffsetDateTime;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(OrderNotFoundException.class)
    public ProblemDetail handleOrderNotFound(OrderNotFoundException ex) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        pd.setType(URI.create("/errors/order-not-found"));
        pd.setProperty("timestamp", OffsetDateTime.now());
        return pd;
    }

    @ExceptionHandler(DuplicateOrderException.class)
    public ProblemDetail handleDuplicateOrder(DuplicateOrderException ex) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, ex.getMessage());
        pd.setType(URI.create("/errors/duplicate-order"));
        pd.setProperty("timestamp", OffsetDateTime.now());
        return pd;
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidation(MethodArgumentNotValidException ex) {
        String detail = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
                .reduce((a, b) -> a + ", " + b)
                .orElse("Validation failed");
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, detail);
        pd.setType(URI.create("/errors/validation-failed"));
        pd.setProperty("timestamp", OffsetDateTime.now());
        return pd;
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleGeneric(Exception ex) {
        log.error("Unhandled exception", ex);
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(
                HttpStatus.INTERNAL_SERVER_ERROR, "An unexpected error occurred");
        pd.setType(URI.create("/errors/internal-error"));
        pd.setProperty("timestamp", OffsetDateTime.now());
        return pd;
    }
}
```

---

### Step 11 - Jackson Config

**`config/JacksonConfig.java`**

```java
package com.orderprocessing.orderservice.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class JacksonConfig {

    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }
}
```

---

### Step 12 - The Order Service (Core Business Logic)

This is the most important class in Phase 2. Read every line.

**`service/OrderService.java`**

```java
package com.orderprocessing.orderservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderprocessing.orderservice.domain.model.*;
import com.orderprocessing.orderservice.domain.repository.OrderRepository;
import com.orderprocessing.orderservice.domain.repository.OutboxEventRepository;
import com.orderprocessing.orderservice.dto.request.CreateOrderRequest;
import com.orderprocessing.orderservice.dto.response.OrderResponse;
import com.orderprocessing.orderservice.event.EventEnvelope;
import com.orderprocessing.orderservice.event.OrderCreatedPayload;
import com.orderprocessing.orderservice.exception.DuplicateOrderException;
import com.orderprocessing.orderservice.exception.OrderNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    // ── Create Order ─────────────────────────────────────────────────────────
    // This method is the core of the Outbox Pattern.
    // Both the order INSERT and the outbox_event INSERT happen inside
    // a single @Transactional boundary. Either both commit or both roll back.
    // There is no scenario where an order exists without a corresponding
    // outbox event, or vice versa.
    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request, String idempotencyKey) {

        // ── Idempotency check ─────────────────────────────────────────────
        // If this idempotency key already exists, return the existing order.
        // This handles retried HTTP requests from the client safely.
        orderRepository.findByIdempotencyKey(idempotencyKey)
                .ifPresent(existing -> {
                    log.info("Duplicate request detected for idempotency key: {}", idempotencyKey);
                    throw new DuplicateOrderException(idempotencyKey);
                });

        // ── Build order ───────────────────────────────────────────────────
        Order order = Order.builder()
                .customerId(request.getCustomerId())
                .idempotencyKey(idempotencyKey)
                .currency(request.getCurrency())
                .build();

        List<OrderItem> items = request.getItems().stream()
                .map(itemReq -> {
                    BigDecimal totalPrice = itemReq.getUnitPrice()
                            .multiply(BigDecimal.valueOf(itemReq.getQuantity()));
                    return OrderItem.builder()
                            .productId(itemReq.getProductId())
                            .productName(itemReq.getProductName())
                            .quantity(itemReq.getQuantity())
                            .unitPrice(itemReq.getUnitPrice())
                            .totalPrice(totalPrice)
                            .build();
                })
                .toList();

        items.forEach(order::addItem);

        BigDecimal totalAmount = items.stream()
                .map(OrderItem::getTotalPrice)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        order.setTotalAmount(totalAmount);

        // ── Persist order ─────────────────────────────────────────────────
        Order savedOrder = orderRepository.save(order);
        log.info("Order saved: id={}, customerId={}", savedOrder.getId(), savedOrder.getCustomerId());

        // ── Write outbox event (same transaction) ─────────────────────────
        OutboxEvent outboxEvent = buildOutboxEvent(savedOrder);
        outboxEventRepository.save(outboxEvent);
        log.info("Outbox event written: eventType=ORDER_CREATED, orderId={}", savedOrder.getId());

        return toResponse(savedOrder);
    }

    // ── Get Order ─────────────────────────────────────────────────────────────
    @Transactional(readOnly = true)
    public OrderResponse getOrder(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        return toResponse(order);
    }

    // ── Update Order Status (called by Kafka consumers) ───────────────────────
    @Transactional
    public void updateOrderStatus(UUID orderId, OrderStatus newStatus) {
        int updated = orderRepository.updateStatus(orderId, newStatus);
        if (updated == 0) {
            log.warn("Status update skipped - order not found or version conflict: {}", orderId);
            return;
        }
        log.info("Order status updated: orderId={}, newStatus={}", orderId, newStatus);
    }

    // ── Private helpers ───────────────────────────────────────────────────────
    private OutboxEvent buildOutboxEvent(Order order) {
        OrderCreatedPayload payload = OrderCreatedPayload.builder()
                .orderId(order.getId())
                .customerId(order.getCustomerId())
                .totalAmount(order.getTotalAmount())
                .currency(order.getCurrency())
                .items(order.getItems().stream()
                        .map(item -> OrderCreatedPayload.OrderItem.builder()
                                .productId(item.getProductId())
                                .productName(item.getProductName())
                                .quantity(item.getQuantity())
                                .unitPrice(item.getUnitPrice())
                                .totalPrice(item.getTotalPrice())
                                .build())
                        .toList())
                .build();

        EventEnvelope envelope = EventEnvelope.builder()
                .eventId(UUID.randomUUID())
                .eventType("ORDER_CREATED")
                .eventVersion("v1")
                .aggregateId(order.getId())
                .aggregateType("ORDER")
                .correlationId(UUID.randomUUID())
                .causationId(null)
                .occurredAt(OffsetDateTime.now())
                .producer("order-service")
                .payload(payload)
                .build();

        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize outbox event payload", e);
        }

        return OutboxEvent.builder()
                .aggregateId(order.getId())
                .aggregateType("ORDER")
                .eventType("ORDER_CREATED")
                .eventVersion("v1")
                .payload(payloadJson)
                .kafkaTopic("order.created")
                .kafkaKey(order.getId().toString())
                .build();
    }

    private OrderResponse toResponse(Order order) {
        return OrderResponse.builder()
                .orderId(order.getId())
                .customerId(order.getCustomerId())
                .status(order.getStatus())
                .totalAmount(order.getTotalAmount())
                .currency(order.getCurrency())
                .createdAt(order.getCreatedAt())
                .updatedAt(order.getUpdatedAt())
                .items(order.getItems().stream()
                        .map(item -> OrderResponse.OrderItemResponse.builder()
                                .productId(item.getProductId())
                                .productName(item.getProductName())
                                .quantity(item.getQuantity())
                                .unitPrice(item.getUnitPrice())
                                .totalPrice(item.getTotalPrice())
                                .build())
                        .toList())
                .build();
    }
}
```

---

### Step 13 - The Outbox Poller

**`outbox/OutboxPoller.java`**

```java
package com.orderprocessing.orderservice.outbox;

import com.orderprocessing.orderservice.domain.model.OutboxEvent;
import com.orderprocessing.orderservice.domain.repository.OutboxEventRepository;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPoller {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final EntityManager entityManager;

    @Value("${outbox.poller.batch-size:10}")
    private int batchSize;

    @Value("${outbox.poller.advisory-lock-id:72857438}")
    private long advisoryLockId;

    @Scheduled(fixedDelayString = "${outbox.poller.fixed-delay-ms:1000}")
    @Transactional
    public void pollAndPublish() {

        // ── Acquire advisory lock ──────────────────────────────────────────
        // pg_try_advisory_xact_lock is a transaction-scoped lock -
        // it is automatically released when this @Transactional method commits.
        // Returns true if acquired, false if another pod holds it.
        Boolean lockAcquired = (Boolean) entityManager
                .createNativeQuery("SELECT pg_try_advisory_xact_lock(:lockId)")
                .setParameter("lockId", advisoryLockId)
                .getSingleResult();

        if (Boolean.FALSE.equals(lockAcquired)) {
            log.debug("Advisory lock not acquired - another instance is polling");
            return;
        }

        List<OutboxEvent> pendingEvents = outboxEventRepository.findPendingEvents(batchSize);

        if (pendingEvents.isEmpty()) {
            return;
        }

        log.info("Outbox poller: processing {} pending events", pendingEvents.size());

        for (OutboxEvent event : pendingEvents) {
            try {
                kafkaTemplate.send(event.getKafkaTopic(), event.getKafkaKey(), event.getPayload())
                        .whenComplete((result, ex) -> {
                            if (ex != null) {
                                log.error("Failed to publish outbox event: id={}, error={}",
                                        event.getId(), ex.getMessage());
                            } else {
                                log.debug("Published outbox event: id={}, topic={}, partition={}, offset={}",
                                        event.getId(),
                                        result.getRecordMetadata().topic(),
                                        result.getRecordMetadata().partition(),
                                        result.getRecordMetadata().offset());
                            }
                        });

                // Mark as PUBLISHED immediately after send is dispatched.
                // If Kafka send fails asynchronously, the next poll cycle
                // will not retry this event - we handle that in a follow-up
                // with a FAILED status and retry_count. For now this is
                // the correct baseline behaviour.
                event.setStatus("PUBLISHED");
                event.setPublishedAt(OffsetDateTime.now());

            } catch (Exception e) {
                log.error("Exception publishing outbox event: id={}", event.getId(), e);
                event.setRetryCount(event.getRetryCount() + 1);
                event.setLastError(e.getMessage());
            }
        }
    }
}
```

---

### Step 14 - Controller

**`controller/OrderController.java`**

```java
package com.orderprocessing.orderservice.controller;

import com.orderprocessing.orderservice.dto.request.CreateOrderRequest;
import com.orderprocessing.orderservice.dto.response.OrderResponse;
import com.orderprocessing.orderservice.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody CreateOrderRequest request) {

        log.info("Received create order request: customerId={}, idempotencyKey={}",
                request.getCustomerId(), idempotencyKey);

        OrderResponse response = orderService.createOrder(request, idempotencyKey);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<OrderResponse> getOrder(@PathVariable UUID orderId) {
        return ResponseEntity.ok(orderService.getOrder(orderId));
    }
}
```

---

### Step 15 - Enable Scheduling

Add `@EnableScheduling` to the main application class:

```java
package com.orderprocessing.orderservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class OrderServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
```

---

### Step 16 - Run the Service

Make sure your infra is running (`docker compose ps` - all healthy), then:

```bash
cd order-service
./mvnw spring-boot:run
```

Watch the startup logs. You should see:

- Flyway running `V1__init_orders_schema.sql` successfully
- `HikariPool` initializing
- `Tomcat started on port 8081`
- No red errors

---

### Step 17 - Smoke Test

```bash
# Create an order
curl -X POST http://localhost:8081/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{
    "customerId": "a0000000-0000-0000-0000-000000000001",
    "items": [
      {
        "productId":   "b0000000-0000-0000-0000-000000000001",
        "productName": "Wireless Headphones",
        "quantity":    2,
        "unitPrice":   1299.99
      }
    ],
    "currency": "INR"
  }'
```

Expected: `202 Accepted` with the order JSON body, status `PENDING`.

Then check the database directly:

```bash
# Confirm order row exists
docker exec orders-db psql -U orders_user -d orders_db \
  -c "SELECT id, status, total_amount, idempotency_key FROM orders;"

# Confirm outbox event was written in the same transaction
docker exec orders-db psql -U orders_user -d orders_db \
  -c "SELECT id, event_type, status, kafka_topic, kafka_key FROM outbox_events;"

# After ~2 seconds, confirm outbox poller published it
docker exec orders-db psql -U orders_user -d orders_db \
  -c "SELECT id, event_type, status, published_at FROM outbox_events;"
```

The outbox event status should change from `PENDING` to `PUBLISHED` within 1–2 seconds. That is your outbox poller working.

---

### Step 18 - Git Commit

```bash
git add .
git commit -m "feat: phase 2 - order service domain layer and outbox pattern

- Flyway migration V1: orders, order_items, outbox_events tables
- Order, OrderItem, OutboxEvent JPA entities with optimistic locking
- OrderService: atomic dual-write (order + outbox event in single transaction)
- OutboxPoller: pg_try_advisory_xact_lock for safe horizontal scaling
- GlobalExceptionHandler with RFC 9457 ProblemDetail responses
- POST /api/v1/orders with Idempotency-Key header support
- GET /api/v1/orders/{orderId}
- EventEnvelope and OrderCreatedPayload for Kafka message contract
"
git push origin main
```

---

Run through all steps, do the smoke test, confirm the outbox event transitions to `PUBLISHED` in the DB, and come back. Phase 3 starts the Inventory Service - the first Kafka consumer.
