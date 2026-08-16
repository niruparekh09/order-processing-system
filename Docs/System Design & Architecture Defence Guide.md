This is the right question to be asking. The project is the vehicle - the answers below are the actual interview weapon. Read every single one of these until you can say them out loud without looking.

---

# System Design & Architecture Defence Guide

## Your Project's Core Interview Narrative

Before any specific question, you need one paragraph that frames everything. Memorise this and adapt it naturally:

> "I built a production-grade event-driven order processing system to deeply understand distributed systems problems - not just implement patterns but understand why they exist. Every design decision in the project was made to solve a specific failure mode. I can walk you through any of them."

That framing immediately signals you as someone who thinks in problems and tradeoffs, not someone who followed a tutorial.

---

## Section 1 - The Outbox Pattern Questions

---

### "Why did you use the Outbox Pattern? Why not just publish to Kafka directly after saving to the database?"

The naive approach has a fundamental flaw called the dual-write problem. If you save to PostgreSQL and then publish to Kafka as two separate operations, you have two possible crash scenarios:

Scenario A - you save to the database successfully, then the process crashes before publishing. The order exists but Kafka never gets the event. Inventory Service never sees it. The order is permanently stuck in PENDING with no path to resolution.

Scenario B - you publish to Kafka first, then crash before saving. The event is in Kafka, Inventory acts on it, but no order record exists in the database. You now have inventory deducted for a non-existent order.

There is no way to make these two operations atomic without a distributed transaction, and distributed transactions across a database and a message broker are extremely expensive and fragile.

The Outbox Pattern solves this by changing what we write to. Instead of writing to Kafka directly, we write to an `outbox_events` table inside the same PostgreSQL database, inside the same ACID transaction as the order insert. Both writes succeed or both roll back together - that's a local transaction, which PostgreSQL handles perfectly. A separate poller then reads from the outbox and publishes to Kafka asynchronously. The poller can crash and restart safely because the outbox event persists until it's marked as published.

---

### "What happens if the Outbox Poller publishes to Kafka but crashes before marking the event as PUBLISHED?"

This is at-least-once delivery and it is a deliberate design choice.

The poller sends the message to Kafka and gets an acknowledgement. Before it can update the outbox event status to PUBLISHED, the process crashes. On restart, the poller sees the event still in PENDING status and publishes it again. Kafka now has two copies of the same `ORDER_CREATED` event.

This is acceptable because every downstream consumer is **idempotent**. The Inventory Service checks Redis and its database for the `eventId` before processing. If it has already processed this event, it discards the duplicate silently. The business outcome is identical to processing it once.

The alternative - exactly-once delivery - requires Kafka transactions with `transactional.id` configured on the producer and `isolation.level=read_committed` on consumers. This is more complex, has higher latency, and is not necessary when consumers are already idempotent. I deliberately chose at-least-once with idempotent consumers because it is simpler and the correctness guarantee is equivalent.

---

### "Why use a polling approach? Isn't that inefficient?"

For this scale, polling every second is perfectly efficient. The query hits a partial index on `WHERE status = 'PENDING'` so it only scans unpublished events. Under low load, most polls return zero rows in under a millisecond.

The more important question is what the alternatives are and why I chose polling:

**SKIP LOCKED** - instead of an advisory lock, you use `SELECT ... FOR UPDATE SKIP LOCKED` which lets multiple pollers run in parallel, each picking up a different batch of events without contention. This is the natural evolution when you need higher throughput.

**Debezium / CDC** - reads PostgreSQL's Write-Ahead Log directly. No polling, no advisory lock, sub-second latency. The outbox event appears in Kafka the moment it's committed to PostgreSQL. This is the production choice at scale but requires Kafka Connect infrastructure.

**Kafka Transactions** - transactional producers that write to Kafka inside a distributed transaction with the database. This eliminates the outbox table entirely but requires database-Kafka distributed transaction support, which only exists in some databases.

I chose polling because it has zero external infrastructure dependencies, is trivially debuggable, and the correctness guarantees are identical to CDC for this scale. I understand the evolution path.

---

### "Why the advisory lock on the poller?"

Without it, if Order Service scales to two pods, both pods run their own `@Scheduled` poller simultaneously. Both read the same 10 PENDING outbox events. Both publish them. Every event is doubled in Kafka.

`pg_try_advisory_xact_lock` is a PostgreSQL session-level non-blocking lock. If Pod A holds it, Pod B's call returns false immediately - it doesn't wait. Pod B skips that poll cycle entirely. Pod A finishes and the lock is automatically released when the transaction commits. On the next cycle, either pod can acquire it.

This gives us safe horizontal scaling of the Order Service with zero external infrastructure - no Redis-based leader election, no Zookeeper, no separate coordination service.

---

## Section 2 - Kafka Questions

---

### "Why Kafka? Why not just use HTTP between services?"

HTTP creates temporal coupling. If Inventory Service is down when Order Service tries to call it, the request fails. Order Service has to either retry immediately (which blocks the thread), implement its own retry logic with backoff (which is complex), or return a failure to the client (which is incorrect - the order should still be processable when Inventory comes back up).

With Kafka, Order Service publishes the event and returns immediately. The event sits in Kafka durably. When Inventory Service comes back online after 30 minutes of downtime, it reads from where it left off using its committed consumer offset. Not a single order is lost. No retry logic required in Order Service.

Additionally, HTTP creates point-to-point coupling. If I want Notification Service to also react to `ORDER_CREATED`, with HTTP I have to add another explicit call in Order Service. With Kafka, Notification Service simply subscribes to the topic. Order Service has no knowledge of how many consumers exist.

The tradeoff is that Kafka introduces operational complexity, makes the request flow harder to trace, and makes the API asynchronous - clients receive 202 Accepted rather than a final outcome. These are real costs that are worth paying at this scale.

---

### "What is your Kafka partitioning strategy and why?"

All topics use `orderId` as the partition key. There are 6 partitions on each business topic.

The reason for `orderId` as the key is ordering guarantees. Kafka guarantees message ordering only within a single partition. By keying on `orderId`, all events for the same order - `ORDER_CREATED`, `INVENTORY_RESERVED`, `PAYMENT_PROCESSED` - land on the same partition. This means if multiple events for the same order are in flight simultaneously, a consumer will always process them in the sequence they were produced.

Six partitions allows up to 6 parallel consumer instances in a consumer group. It's divisible by 2 and 3, making it easy to scale consumer pods to 2, 3, or 6 instances with perfectly balanced partition assignment. Partition count cannot be reduced after creation, so choosing a reasonable number upfront is important.

DLQ topics have 1 partition because they are low-volume, sequential by nature, and simplify operational tooling - an ops engineer consuming from DLQ processes events one at a time.

---

### "What happens during a Kafka consumer rebalance?"

A rebalance occurs when a consumer joins or leaves a consumer group - during deployments, crashes, or scaling events. During a rebalance, all consumers in the group stop processing and partitions are redistributed.

The risk is that a consumer is in the middle of processing a message when the rebalance starts. If it has already committed the offset but hasn't finished processing, the message is lost. If it hasn't committed the offset and another consumer picks up the same partition, the message is reprocessed.

My configuration addresses this in three ways. First, `enable-auto-commit: false` - offsets are committed only after successful processing, preventing the lost message scenario. Second, all consumers are idempotent - reprocessed messages produce the same outcome. Third, the `@KafkaListener` uses Spring's `DefaultErrorHandler` which handles exceptions before offset commit, routing failed messages to DLQ after exhausting retries rather than committing a bad offset.

---

### "How do you handle poison messages?"

A poison message is one that causes the consumer to throw an exception every time it tries to process it - malformed payload, unexpected null, schema version mismatch. Without handling, the consumer will retry infinitely, blocking all subsequent messages on that partition.

My approach uses Spring Kafka's `DefaultErrorHandler` with a `DeadLetterPublishingRecoverer`. After a configured number of retry attempts with exponential backoff - 3 attempts, backoff doubling from 1 to 4 seconds - the message is forwarded to the corresponding DLQ topic with additional headers containing the original topic, partition, offset, and the exception message. The consumer then commits the offset and continues processing subsequent messages.

DLQ events are retained for 30 days. An operator can inspect them, fix the root cause, and replay them either manually or via an automated DLQ processor.

---

### "What is consumer lag and how do you monitor it?"

Consumer lag is the difference between the latest offset in a partition and the last committed offset of a consumer group. It tells you how far behind the consumer is from real-time.

A lag of zero means the consumer is processing messages as fast as they arrive. A growing lag means the consumer cannot keep up with the producer - either the producer is publishing faster than the consumer can handle, or the consumer is slow due to downstream dependencies.

I expose consumer lag via Micrometer's Kafka metrics and visualise it in Grafana. A lag alert firing means either I need to scale the consumer group (add more pods, up to the partition count), the downstream dependency (database, Redis) is slow, or there is a poison message blocking a partition.

---

## Section 3 - Saga & Distributed Transactions

---

### "Why choreography instead of orchestration?"

Both patterns solve the same problem - coordinating a multi-step distributed transaction - but with different tradeoffs.

**Orchestration** uses a central Saga Orchestrator service that explicitly tells each participant what to do next: "Inventory Service, reserve stock for order X. Payment Service, charge card for order X." The orchestrator maintains the saga state and handles failures explicitly. The flow is easy to visualise from one place.

The problem is the orchestrator becomes a single point of failure. If it goes down, all in-flight sagas are blocked. It also becomes a bottleneck - every step in every order flows through it. It creates tight coupling - adding a new step means modifying the orchestrator.

**Choreography** distributes the coordination logic into each service. Inventory Service knows: when I see `ORDER_CREATED`, I reserve stock and publish the outcome. Payment Service knows: when I see `INVENTORY_RESERVED`, I process payment. No service has global knowledge of the flow.

I chose choreography because it aligns with the autonomy principle of microservices - each service is independently deployable and has no runtime dependency on a central coordinator. The tradeoff is that the overall flow is harder to visualise from any single service's code, which is why distributed tracing via Zipkin and the sequence diagrams in the README exist.

For a more complex saga with many steps, conditional branching, or compensation that requires coordination across many services, orchestration becomes the better choice. For three services with a linear flow, choreography is appropriate.

---

### "What happens if Inventory Service is unavailable for 30 minutes?"

The `ORDER_CREATED` events accumulate in Kafka. Kafka retains them per the topic's retention policy - 7 days in my configuration. Inventory Service's consumer group offset does not advance.

When Inventory Service comes back online, it resumes consuming from its last committed offset. It processes the backlog in sequence, and the saga continues for each order exactly as if the downtime never happened.

Order Service sees no impact during the outage. It accepted the orders, persisted them, and published the events. Its responsibility is complete.

The only user-visible impact is latency - orders placed during the outage take longer to reach a terminal state. This is the fundamental tradeoff of eventual consistency: the system remains available and consistent but not necessarily responsive in real time.

If the business requirement is that orders must be processed within N minutes or cancelled, I would add a scheduled job in Order Service that finds orders in PENDING state older than N minutes and transitions them to CANCELLED, publishing a compensating event.

---

### "What if payment succeeds but the payment confirmation event is lost?"

This is the most dangerous failure scenario in the system and it's worth being precise about.

Scenario: Payment Service charges the customer's card successfully. It then tries to publish `PAYMENT_PROCESSED` to Kafka. The pod crashes before the publish succeeds.

The payment has been taken. The order is stuck in `INVENTORY_RESERVED`. The customer is charged but gets no confirmation.

**How I handle it:** Payment Service also uses the Outbox Pattern. The payment record is written to the `payments_db` and the `PAYMENT_PROCESSED` event is written to an outbox table in the same transaction. If the pod crashes before publishing, the outbox poller on restart picks up the pending event and publishes it. The customer's order completes.

This is why the Outbox Pattern is not just an Order Service concern - every service that produces events should implement it. The payment service outbox closes the gap the question is pointing at.

---

### "What happens if Redis says the event was processed but the DB transaction rolled back?"

This is the reverse of the question above - the fast path idempotency check says "already processed" but the database says "not committed."

Scenario: Inventory Service reads `ORDER_CREATED`. It sets the Redis idempotency key. It tries to update the database but the transaction rolls back due to an optimistic lock conflict. The Redis key now says "processed" but the database has no reservation.

This is why Redis is the fast path and the database is the correctness boundary.

The fix is the order of operations: **set the Redis key only after the database transaction commits successfully, not before.** In the implementation, the Redis `SET NX EX` call happens after `@Transactional` method returns successfully. If the transaction rolls back, the Redis key is never set. The next delivery of the same event will attempt the database operation again.

This is also why I use `SET NX EX` as a single atomic Redis operation rather than `GET` then `SET` - two separate operations can race under concurrent duplicate deliveries.

---

## Section 4 - Idempotency & Concurrency

---

### "Why two layers of idempotency? Redis and database constraint?"

Each layer protects against a different failure mode.

**Redis** is the fast path. It is checked before any business logic executes. If the key exists, the consumer returns immediately - no database access, no business logic, minimal overhead. Redis handles the common case: Kafka delivers the same message twice in quick succession.

**Database UNIQUE constraint** is the correctness guarantee. It handles cases that Redis cannot: events delivered after the Redis key's TTL has expired (24 hours), events delivered to a consumer that restarted and lost its Redis state, and the race window between a successful DB commit and a Redis write failure.

If Redis goes down, the system falls back to database-only idempotency. Requests are slower (no fast-path check) but still correct. This is the right failure mode - degrade gracefully, never lose correctness.

---

### "Why optimistic locking instead of SELECT FOR UPDATE?"

Both prevent overselling but have different performance characteristics.

`SELECT FOR UPDATE` is pessimistic locking. When Inventory Service reads a product row to check stock, it acquires an exclusive lock. Any other transaction trying to read that row for an update must wait. Under high concurrency - 100 simultaneous orders for the same product - 99 transactions are blocked waiting for the lock. Throughput collapses.

Optimistic locking uses a `version` column. No lock is held at read time. The update includes `WHERE version = :readVersion`. If another transaction updated the row between your read and your update, the version has changed and your update affects 0 rows. You detect the conflict and retry.

The tradeoff: optimistic locking has no overhead under low contention and degrades gracefully under high contention by causing retries rather than blocking. Pessimistic locking has guaranteed success on the first try but causes serialization under high contention.

For inventory reservation where conflicts are relatively infrequent - most orders are for different products - optimistic locking is the correct choice. If you have a flash sale where thousands of orders hit the same product simultaneously, you would see many retries, but the system remains correct and available rather than deadlocked.

---

### "What if two concurrent requests both pass the Redis idempotency check simultaneously before either writes the key?"

This is a real race condition if you implement idempotency as `GET key → if null, process → SET key`. Two threads can both GET and see null, both decide to process, and both set the key after.

The fix is using `SET key value NX EX ttl` as a single atomic Redis operation. `NX` means "set only if not exists." Redis executes this atomically - only one thread can win the SET NX. The other gets a false return and knows the event is already being processed.

This is why the implementation uses `RedisTemplate.opsForValue().setIfAbsent(key, value, duration)` which maps directly to `SET NX EX`. Never implement idempotency with separate GET and SET operations.

---

## Section 5 - Observability & Debugging

---

### "How does your correlation ID propagate across asynchronous boundaries?"

The correlation ID originates at the API Gateway. If the incoming HTTP request carries an `X-Correlation-ID` header, the gateway uses it. If not, the gateway generates a UUID and injects it.

In Order Service, the correlation ID is extracted from the request header and stored in the MDC (Mapped Diagnostic Context) - a thread-local map that Logback includes in every log line automatically. It is also included in the `EventEnvelope` payload of every Kafka message.

When Inventory Service consumes the `ORDER_CREATED` event, it extracts the `correlationId` from the envelope and adds it to its own MDC before any processing begins. Every log line Inventory Service writes for that event carries the same correlation ID.

The result: if a single order's processing goes wrong, you can search any log aggregation tool for `correlationId=<uuid>` and get every log line from every service across every Kafka hop, in chronological order, for that specific order's journey.

Zipkin's distributed tracing works similarly via B3 propagation headers, giving you the visual span tree. The correlation ID in logs and the trace ID in Zipkin are complementary - Zipkin shows latency and flow, logs show business context.

---

### "How would you debug an order that's stuck in PENDING?"

This is a practical operations question and the answer demonstrates end-to-end system understanding.

Step 1 - Check the order record in the database. Note the `created_at` timestamp. If it's recent, the system may still be processing. If it's been more than a few seconds, something is wrong.

Step 2 - Check the `outbox_events` table for an event with this `aggregate_id`. If the outbox event status is PENDING, the poller hasn't published it - check if Kafka is reachable and if the advisory lock is being held correctly.

Step 3 - If the outbox event is PUBLISHED, the event reached Kafka. Check Kafka consumer group lag for `inventory-service` using `kafka-consumer-groups --describe`. If lag is growing, Inventory Service is not consuming - check its logs.

Step 4 - Search logs across all services by `correlationId`. The last log line will tell you exactly where the flow stopped.

Step 5 - Check DLQ topics for the order's events. If a message ended up in DLQ, read the error headers to understand why processing failed.

---

## Section 6 - Resilience

---

### "What happens when Inventory Service is processing an event and your Circuit Breaker opens?"

The circuit breaker in my system wraps the external payment gateway call in Payment Service, not internal service-to-service communication (which goes through Kafka, not HTTP).

When the payment gateway starts failing - timeouts, 5xx responses - Resilience4j counts failures against the configured threshold (50% failure rate over 10 calls). When the threshold is crossed, the circuit breaker moves to OPEN state.

In OPEN state, all calls to the payment gateway fail immediately without attempting the network call. This does two things: it protects the payment gateway from being overwhelmed by retries from a struggling system, and it frees Payment Service threads immediately rather than holding them blocked on a connection timeout.

After a configured wait (30 seconds), the breaker moves to HALF_OPEN and allows a probe request through. If it succeeds, the breaker closes. If it fails, it opens again.

During the open period, Payment Service publishes `PAYMENT_FAILED` events, which trigger the compensating transaction path - Inventory releases stock, orders are marked PAYMENT_FAILED. These can be retried by the customer once the payment gateway recovers.

---

### "What is backpressure and how does your system handle it?"

Backpressure is what happens when a consumer cannot process messages as fast as the producer produces them. Consumer lag grows. In the worst case, if Kafka retention expires, messages are lost.

My system handles backpressure at several levels.

At the Kafka consumer level: Spring Kafka's `max.poll.records` limits how many records are fetched per poll. If processing is slow, fewer records are fetched, reducing the memory pressure on the consumer.

At the database level: HikariCP connection pool with `maximum-pool-size=20`. If all connections are in use, new processing threads block rather than opening unbounded connections that overwhelm PostgreSQL.

At the service level: if Inventory Service is slow due to database load, the consumer lag on `order.created` grows. Prometheus alerts on this metric. The response is horizontal scaling - add more Inventory Service pods (up to the partition count of 6).

The architectural response to sustained backpressure is to increase the consumer group size, tune batch processing, or investigate the downstream bottleneck. Consumer lag as a Grafana alert is the early warning system.

---

## Section 7 - Scaling & Evolution Questions

---

### "How would you scale this from 1,000 to 100,000 orders per second?"

This is a multi-layer answer. Deliver it in layers - don't try to say everything at once.

**Layer 1 - Kafka throughput:** increase partition count on all topics (requires topic recreation or careful migration). More partitions allow more parallel consumers. At 100K/s you likely need 50-100 partitions and corresponding consumer instances.

**Layer 2 - Database:** the single biggest bottleneck at scale. Strategies in order of complexity: connection pool tuning, read replicas for GET /orders queries, table partitioning on `orders` by `created_at` (range partitioning), eventually sharding by `customer_id`. The outbox table becomes hot under high write load - archive published events aggressively.

**Layer 3 - Outbox Poller:** polling becomes a bottleneck. Migrate to CDC via Debezium - it reads PostgreSQL WAL directly and can handle millions of events per second with sub-second latency. No polling, no advisory lock needed.

**Layer 4 - Redis:** move to Redis Cluster for horizontal scaling of idempotency key storage. At 100K/s, a single Redis instance becomes the bottleneck.

**Layer 5 - Stateless services:** all four services are stateless - scale horizontally without limit up to the Kafka partition count constraint.

---

### "How would you add a new service to the saga - say, a Fraud Detection service between Order and Inventory?"

This is a choreography extensibility question.

With choreography, adding Fraud Detection Service is additive - no existing service needs to change.

Fraud Detection subscribes to `order.created`. It performs its checks and publishes either `fraud.check.passed` or `fraud.check.failed`. Inventory Service stops listening to `order.created` and starts listening to `fraud.check.passed`. If fraud is detected, Order Service listens to `fraud.check.failed` and marks the order CANCELLED.

With orchestration, you would modify the orchestrator to add the new step - a single code change in one service, but it requires redeployment of the orchestrator.

Choreography's extensibility advantage is real but comes with a visibility cost - the flow is now distributed across four services with no single place showing the complete sequence. This is why tooling like Zipkin and comprehensive sequence diagrams are not optional in a choreography-based system.

---

### "How would you handle schema evolution in your event payloads?"

Every event envelope includes `eventVersion`. Currently all events are `v1`.

When a breaking change is needed - say, adding a required field to `OrderCreatedPayload` - the approach is:

**Step 1:** deploy the new version of Order Service that publishes `v2` events. It continues publishing `v1` events for a transition period, or publishes `v2` and lets consumers handle both.

**Step 2:** update each consumer to handle both `v1` and `v2` by switching on `eventVersion`. `v1` events use the old parsing logic. `v2` events use the new logic.

**Step 3:** once all consumers are deployed and handling `v2`, stop publishing `v1`.

**Step 4:** after the `v1` retention period expires, remove the `v1` handling code.

The alternative for teams that need stronger guarantees is a Schema Registry (Confluent Schema Registry with Avro or Protobuf). The registry enforces schema compatibility rules - backward, forward, or full compatibility - and rejects schema changes that would break consumers. At scale this is the right choice. For this system, the `eventVersion` field is the lightweight equivalent.

---

## Section 8 - Questions Specific to Your Background

Given you'll be at 3 YOE interviewing for SDE-2, expect these situational questions:

---

### "You're working on a monolith at TTL currently. How does this project inform how you'd approach decomposing that monolith?"

> "Working on the monolith showed me exactly the problems that event-driven architecture solves - tight coupling between modules, inability to deploy one feature without risking another, database contention between unrelated operations. Building this project taught me that decomposition isn't about splitting code, it's about identifying transaction boundaries. The right question is always: which operations genuinely need to be atomic and which can be eventually consistent? In the monolith, everything is synchronous because it's easy. But the cost is coupling. The Outbox Pattern, for example, was specifically designed for the transition period when you've split a service but can't yet fully decouple it - you use the old database as a bridge."

---

### "This project uses many patterns. Which one do you think was most difficult to get right and why?"

The honest answer is idempotency - specifically the interaction between Redis and the database.

> "The pattern itself is simple to understand but getting the atomicity right is subtle. The naive implementation - GET from Redis, if empty then process, then SET in Redis - has a race condition under concurrent duplicate messages. The fix, using SET NX EX as a single atomic operation and only setting it after the database transaction commits, seems obvious in hindsight but requires you to think carefully about the failure modes. What if the Redis write fails after the DB commits? You've processed the event and have no record in Redis. The next delivery will try to process it again. Your database constraint is your final safety net. Understanding that the database constraint is the correctness guarantee and Redis is only a performance optimisation - that mental model took time to arrive at."

---

## What to Study Beyond This Project

The project covers HLD well. To be in the top 5% at 3 YOE, fill these gaps:

**Database internals** - understand B-tree indexes, how `EXPLAIN ANALYZE` works, what a sequential scan vs index scan means, what the query planner does. Be able to look at a slow query and know why it's slow. This comes up constantly.

**CAP theorem in practice** - not the theorem itself, everyone knows it. The practical version: "when your network partitions, do you want CP (reject writes to stay consistent) or AP (accept writes and reconcile later)?" Know where your system falls and why. PostgreSQL is CP. Kafka is AP with configurable durability.

**Java concurrency** - `synchronized`, `ReentrantLock`, `CompletableFuture`, `@Async`, thread pools. At SDE-2 level they will ask you about a concurrency bug in code and expect you to identify it. Practice reading concurrent code and spotting races.

**JVM internals** - GC basics (what is a GC pause, why does it matter for latency), heap vs stack, when does an OutOfMemoryError happen. Not deep but enough to answer "your service has high latency spikes every few minutes - what would you look at?"

**Spring internals** - what does `@Transactional` actually do (proxy, AOP), why does `@Transactional` on a private method not work, what is the difference between `REQUIRES_NEW` and `REQUIRED`. These are standard SDE-2 Java interview questions.

**Redis data structures** - know when to use String vs Hash vs Sorted Set vs List. The idempotency key use case is String with TTL. Rate limiting is a Counter or Sorted Set. Leaderboards are Sorted Set. Caching is Hash or String. Be able to map a problem to the right structure.

**Load testing** - run k6 or JMeter against your own project. Submit 500 concurrent order creation requests. Measure P50/P95/P99. Find the bottleneck. Fix it or explain why you'd fix it. Having real numbers from your own system is a conversation-stopper in a good way.

---

## The One Meta-Skill That Gets Offers

Every question above follows the same structure. Practice giving answers in this shape:

**Problem** → what failure or limitation does this solve?
**Naive solution** → what's the obvious approach and why does it fail?
**Chosen solution** → what did you do and exactly how does it solve the problem?
**Tradeoffs** → what does your solution cost you?
**Alternatives** → what else could you have done and when would you choose it instead?
**Failure modes** → what can still go wrong and how do you detect/handle it?

An interviewer asking "why choreography?" is not looking for the definition of choreography. They are checking if you can think through a problem systematically. If your answer includes a tradeoff and a scenario where you'd choose differently, you are demonstrating SDE-2 level thinking. That is what gets the offer.