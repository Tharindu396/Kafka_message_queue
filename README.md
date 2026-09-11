# Kafka Order Pipeline — Avro, Real-time Aggregation, Retry & DLQ

A Kafka system that produces and consumes **order** messages serialised with **Apache Avro**
(via the Confluent Schema Registry), and supports:

| Requirement                                       | Where it lives                               | How to test it manually                                                                                     |
| ------------------------------------------------- | -------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Avro serialization                                | `src/main/avro/order.avsc` + Schema Registry | [§7.1](#71-test-1--avro-serialization)                                                                      |
| Real-time aggregation (running average of prices) | `aggregation/PriceAggregator.java`           | [§7.2](#72-test-2--real-time-aggregation-running-average)                                                   |
| Retry logic for temporary failures                | `config/KafkaErrorHandlingConfig.java`       | [§7.4](#74-test-4--retry-that-recovers)                                                                     |
| Dead Letter Queue                                 | same file + `dlq/DlqConsumer.java`           | [§7.3](#73-test-3--permanent-failure--dlq-with-no-retry), [§7.6](#76-test-6--inspect-the-dead-letter-queue) |

**Stack:** Java 21 · Spring Boot 3.3 · Spring Kafka · Avro 1.11 · Confluent Schema Registry 7.7 · Docker Compose

---

## 1. Architecture

```
   Insomnia / curl
   POST /api/orders
         |
         v
+----------------+  Avro   +-------------+       +----------------+
| OrderProducer  |-------->|   orders    |------>| OrderConsumer  |--> PriceAggregator
| (HTTP or timer)|         | 3 partitions|       +--------+-------+    (count, sum,
+----------------+         +-------------+                |             running average,
         |                                                |             per product)
         | registers schema           transient error ----+
         v                            retry 0.5s->1s->2s  |
+------------------+                                      | permanent error,
| Schema Registry  |                                      | or retries exhausted
|      :8081       |                                      v
+------------------+                              +----------------+
                                                  |  orders.DLQ    |--> DlqConsumer
                                                  +----------------+    |
                                                                        v
                                                              GET /api/dlq
                                                        (payload + failure headers)
```

### Message schema (`order.avsc`)

| Field     | Type   | Description                                     |
| --------- | ------ | ----------------------------------------------- |
| `orderId` | string | Unique identifier for the order (e.g. `"1001"`) |
| `product` | string | Name of the purchased item (e.g. `"Item1"`)     |
| `price`   | float  | Price of the product (randomised)               |

The Avro Java class `com.assignment.orders.avro.Order` is **generated at build time** from this file
by the `avro-maven-plugin` — the schema is the single source of truth, never a hand-written POJO.

On the wire each message is `[magic byte][4-byte schema id][Avro binary payload]`; the schema itself
is stored once in the Schema Registry rather than repeated in every message.

---

## 2. Prerequisites

- Docker Desktop
- JDK 21
- Maven 3.9+
- Insomnia (or Postman / curl) for sending test requests

---

## 3. Run it

```bash
# 1. Kafka (KRaft mode, no ZooKeeper) + Schema Registry + Kafka UI
docker compose up -d

# 2. Build - this also generates Order.java from order.avsc
mvn clean package

# 3. Run
mvn spring-boot:run
```

### Ports

| Service         | URL                     | What it is                                                    |
| --------------- | ----------------------- | ------------------------------------------------------------- |
| **Order API**   | <http://localhost:8090> | The app's own HTTP endpoints — this is what Insomnia talks to |
| Kafka UI        | <http://localhost:8080> | Browse topics, messages, schemas, consumer groups             |
| Schema Registry | <http://localhost:8081> | REST API for registered Avro schemas                          |
| Kafka broker    | `localhost:9092`        | Kafka's own binary protocol (not HTTP)                        |

> **Kafka does not speak HTTP**, so Insomnia cannot talk to the broker directly. The app exposes a
> small REST layer (`/api/...`) that publishes real Avro messages through the same producer the rest
> of the system uses — a manual request exercises the real code path, not a test-only shortcut.

### Profiles — important for manual testing

By default the app runs three profiles: `producer`, `consumer`, `dlq`. The `producer` profile fires
an order every 0.8 seconds, which floods the console.

**For manual testing, turn the automatic producer off** so that every line you see is caused by a
request _you_ sent:

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=consumer,dlq
```

| Profile    | Effect                                                            |
| ---------- | ----------------------------------------------------------------- |
| `producer` | Auto-generates an order every 0.8s (leave OFF for manual testing) |
| `consumer` | Consumes `orders`, runs the aggregation, applies retry/DLQ        |
| `dlq`      | Tails `orders.DLQ` and records dead letters for `GET /api/dlq`    |

Tear down with `docker compose down -v`.

---

## 4. How each requirement is implemented

### 4.1 Avro serialization

- Producer uses `KafkaAvroSerializer` and auto-registers the schema under subject `orders-value`.
- Consumer uses `KafkaAvroDeserializer` with `specific.avro.reader=true`, so the listener receives a
  typed `Order`, not a `GenericRecord`.
- That deserializer is wrapped in Spring's `ErrorHandlingDeserializer`, so a corrupt or non-Avro
  **poison pill** cannot kill the consumer — it is routed to the DLQ instead of causing an infinite
  crash loop.

### 4.2 Real-time aggregation

`PriceAggregator` keeps a running `count` and `sum` and derives `average = sum / count`
incrementally — O(1) per message, with no re-reading of history. Every consumed order prints the
updated average, a summary block prints every 15 seconds, and `GET /api/stats` returns the same
numbers as JSON.

Only **successfully processed** orders are counted: an order that recovers on retry is counted
exactly once, and an order that ends in the DLQ is never counted.

### 4.3 Retry logic for temporary failures

Failures are classified by exception type. The listener itself contains **no try/catch** — the policy
lives in one place, the container's `DefaultErrorHandler`:

| Exception                      | Behaviour                                                                |
| ------------------------------ | ------------------------------------------------------------------------ |
| `TransientProcessingException` | Retried in place, exponential backoff **0.5s → 1s → 2s** (max 3 retries) |
| `PermanentProcessingException` | Registered as _not retryable_ → straight to the DLQ                      |
| `DeserializationException`     | Not retryable → straight to the DLQ                                      |

Offsets are committed manually (`enable-auto-commit: false`, `ack-mode: record`) **after** a record
has either succeeded or been parked in the DLQ, so a crash mid-retry cannot silently lose a message.

### 4.4 Dead Letter Queue

`DeadLetterPublishingRecoverer` republishes the failed record to `orders.DLQ` on the **same
partition**, with diagnostic headers:

| Header                                                | Meaning                                                     |
| ----------------------------------------------------- | ----------------------------------------------------------- |
| `x-failure-class`                                     | `PERMANENT` / `TRANSIENT_RETRIES_EXHAUSTED` / `POISON_PILL` |
| `kafka_dlt-original-topic` / `-partition` / `-offset` | Where the message came from                                 |
| `kafka_dlt-exception-fqcn` / `-exception-message`     | What went wrong                                             |
| `x-failed-at`                                         | Timestamp of the final failure                              |

Two producer templates are registered, because a DLQ record can be either a deserialised `Order`
(Avro template) or raw bytes that never deserialised (byte[] template).

---

## 5. Failure injection — the trigger values

The consumer reacts to specific field values, which is what makes every path testable on demand:

| Send this                 | Consumer behaviour                         | Outcome                        |
| ------------------------- | ------------------------------------------ | ------------------------------ |
| `price` ≤ 0               | `PermanentProcessingException`             | **DLQ immediately, no retry**  |
| `product` = `ALWAYS-FAIL` | `TransientProcessingException`, every time | 4 attempts → **DLQ**           |
| `product` = `FLAKY-ITEM`  | fails twice, succeeds on the 3rd attempt   | **recovers via retry**         |
| anything else             | success                                    | counted in the running average |

When the `producer` profile is on, it also injects these automatically (every 11th, 7th and 5th
order respectively) so an unattended run still exercises every path.

---

## 6. HTTP API reference

Base URL: `http://localhost:8090`

| Method | Path          | Purpose                                                                |
| ------ | ------------- | ---------------------------------------------------------------------- |
| `POST` | `/api/orders` | Publish one Avro order. Returns the partition and offset it landed on  |
| `GET`  | `/api/stats`  | Live running aggregation (count, total, average, min/max, per product) |
| `GET`  | `/api/dlq`    | Dead letters seen so far, newest first, **with their failure headers** |

**Insomnia setup:** create a Request Collection, then add an environment with
`{ "base_url": "http://localhost:8090" }` so each request can use `{{ base_url }}/api/orders`.
For `POST`, set the body type to **JSON**.

---

## 7. Manual testing guide

Two things open:

1. **A terminal** running `mvn spring-boot:run -Dspring-boot.run.profiles=consumer,dlq`
2. **Insomnia**, pointed at `http://localhost:8090`

Each test below is one request. Watch both the JSON response _and_ the terminal.

---

### 7.1 Test 1 — Avro serialization

**Request** — `GET http://localhost:8081/subjects`

```json
["orders-value", "orders.DLQ-value"]
```

**Request** — `GET http://localhost:8081/subjects/orders-value/versions/1`

```json
{
  "subject": "orders-value",
  "version": 1,
  "id": 1,
  "schema": "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"com.assignment.orders.avro\",\"fields\":[{\"name\":\"orderId\",\"type\":\"string\"},{\"name\":\"product\",\"type\":\"string\"},{\"name\":\"price\",\"type\":\"float\"}]}"
}
```

✅ **What this proves:** the schema is registered centrally with the three required fields, and
`price` really is an Avro `float`. Messages on the topic carry only a 4-byte schema id, not the
schema text.

---

### 7.2 Test 2 — Real-time aggregation (running average)

**Request** — `POST {{ base_url }}/api/orders`, body → JSON:

```json
{ "orderId": "9001", "product": "Laptop", "price": 1250.5 }
```

**Response** `201 Created`:

```json
{
  "status": "published",
  "topic": "orders",
  "partition": 0,
  "offset": 0,
  "order": { "product": "Laptop", "orderId": "9001", "price": 1250.5 }
}
```

**Terminal:**

```
CONSUME | orderId=9001 product=Laptop price=1250.50 | p0@0 | count=1 RUNNING AVG=1250.50
```

Now send a second one:

```json
{ "orderId": "9002", "product": "Mouse", "price": 249.5 }
```

```
CONSUME | orderId=9002 product=Mouse price=249.50 | p1@0 | count=2 RUNNING AVG=750.00
```

**Request** — `GET {{ base_url }}/api/stats`

```json
{
  "ordersProcessed": 2,
  "totalValue": 1500.0,
  "runningAverage": 750.0,
  "minPrice": 249.5,
  "maxPrice": 1250.5,
  "byProduct": {
    "Laptop": { "count": 1, "average": 1250.5 },
    "Mouse": { "count": 1, "average": 249.5 }
  }
}
```

✅ **What this proves:** `(1250.50 + 249.50) / 2 = 750.00`. Say the arithmetic out loud during the
demo — the average is recomputed on every single message, not batched.

---

### 7.3 Test 3 — Permanent failure → DLQ with no retry

**Request** — `POST {{ base_url }}/api/orders`

```json
{ "orderId": "9003", "product": "Keyboard", "price": -1 }
```

The response is still `201` — publishing succeeded; it is the _consumer_ that rejects it.

**Terminal:**

```
NO-RETRY| key=9003 offset=0 is unrecoverable, skipping straight to DLQ : invalid price -1.0
DLQ     | giving up on key=9003 offset=0 -> orders.DLQ
**************** DEAD LETTER #1 ****************
  payload  : {"orderId": "9003", "product": "Keyboard", "price": -1.0}
  x-failure-class              : PERMANENT
```

✅ **What this proves:** there are **no `RETRY` lines at all**. Bad data is never retried — retrying a
validation error only wastes time and delays every message behind it.

**Also check** `GET {{ base_url }}/api/stats` — `ordersProcessed` is still **2**. A failed order
never pollutes the average.

---

### 7.4 Test 4 — Retry that recovers

**Request** — `POST {{ base_url }}/api/orders`

```json
{ "orderId": "9005", "product": "FLAKY-ITEM", "price": 500 }
```

**Terminal:**

```
RETRY   | attempt 1/4 failed for key=9005 : database timeout on order 9005 (attempt 1)
RETRY   | attempt 2/4 failed for key=9005 : database timeout on order 9005 (attempt 2)
CONSUME | orderId=9005 product=FLAKY-ITEM price=500.00 | p0@1 | count=3 RUNNING AVG=666.67
```

✅ **What this proves:** two failures, then success. **Point at the timestamps** — the gap between
attempts grows (0.5s, then 1s). That is the exponential backoff, visible.

**Also check** `GET {{ base_url }}/api/stats` — `ordersProcessed` is **3**, not 5. The order was
counted **once** despite being delivered three times. Retry did not corrupt the aggregation.

---

### 7.5 Test 5 — Retries exhausted → DLQ

**Request** — `POST {{ base_url }}/api/orders`

```json
{ "orderId": "9004", "product": "ALWAYS-FAIL", "price": 99.99 }
```

**Terminal:**

```
RETRY   | attempt 1/4 failed for key=9004 : pricing service unavailable
RETRY   | attempt 2/4 failed for key=9004 : pricing service unavailable
RETRY   | attempt 3/4 failed for key=9004 : pricing service unavailable
RETRY   | attempt 4/4 failed for key=9004 : pricing service unavailable
DLQ     | giving up on key=9004 -> orders.DLQ
```

✅ **What this proves:** the retry budget is finite. A permanently-broken downstream cannot block the
partition forever — after 4 attempts the message is parked and the consumer moves on.

---

### 7.6 Test 6 — Inspect the Dead Letter Queue

**Request** — `GET {{ base_url }}/api/dlq`

```json
{
  "topic": "orders.DLQ",
  "count": 2,
  "deadLetters": [
    {
      "sequence": 2,
      "key": "9004",
      "payload": "{\"orderId\": \"9004\", \"product\": \"ALWAYS-FAIL\", \"price\": 99.99}",
      "dlqPartition": 2,
      "dlqOffset": 1,
      "failure": {
        "x-failure-class": "TRANSIENT_RETRIES_EXHAUSTED",
        "kafka_dlt-original-topic": "orders",
        "kafka_dlt-original-partition": "2",
        "kafka_dlt-original-offset": "1",
        "kafka_dlt-exception-fqcn": "org.springframework.kafka.listener.ListenerExecutionFailedException",
        "kafka_dlt-exception-message": "... threw exception; pricing service unavailable for order 9004",
        "x-failed-at": "2026-09-10T05:55:33.112530600Z"
      }
    },
    {
      "sequence": 1,
      "key": "9003",
      "payload": "{\"orderId\": \"9003\", \"product\": \"Keyboard\", \"price\": -1.0}",
      "failure": {
        "x-failure-class": "PERMANENT",
        "kafka_dlt-original-offset": "0",
        "kafka_dlt-exception-message": "... threw exception; invalid price -1.0 for order 9003"
      }
    }
  ]
}
```

✅ **What this proves:** the DLQ is not a bin of anonymous bytes. Every dead letter carries **why** it
failed and **exactly which record** it was (original topic, partition, offset) — enough to replay it
once the cause is fixed.

Note the two different `x-failure-class` values: one message was rejected instantly, the other only
after exhausting its retries. Same destination, different reasons.

---

### 7.7 Test 7 — Poison pill (the one test Insomnia cannot do)

A message that is not Avro at all cannot be sent through `/api/orders`, because that endpoint always
produces valid Avro. Send it straight to the broker instead:

```bash
echo "this is not avro" | docker exec -i kafka kafka-console-producer \
  --bootstrap-server kafka:29092 --topic orders
```

Then `GET {{ base_url }}/api/dlq`:

```json
{
  "sequence": 3,
  "key": null,
  "payload": "<undecodable: SerializationException - 16 raw bytes>",
  "failure": {
    "x-failure-class": "POISON_PILL",
    "kafka_dlt-exception-fqcn": "org.springframework.kafka.support.serializer.DeserializationException",
    "kafka_dlt-exception-message": "failed to deserialize"
  }
}
```

✅ **What this proves:** the consumer keeps running. Without `ErrorHandlingDeserializer` this single
message would fail deserialisation, never advance the offset, and block that partition **forever** —
the classic Kafka poison-pill outage. Send a valid order afterwards to show it is still alive.

---

### 7.8 Test 8 — Input validation

**Request** — `POST {{ base_url }}/api/orders` with a field missing:

```json
{ "orderId": "9006", "product": "Desk" }
```

**Response** `400 Bad Request`:

```json
{ "status": "rejected", "errors": { "price": "price is required" } }
```

Note `price` is **not** validated as positive — sending `-1` has to reach Kafka, because that is how
[§7.3](#73-test-3--permanent-failure--dlq-with-no-retry) demonstrates the permanent-failure path.

---

## 8. What to check in Kafka UI (<http://localhost:8080>)

| #   | Navigate to                        | What to look for                                                                                                   | Why it matters                                                                                                                  |
| --- | ---------------------------------- | ------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| 1   | **Dashboard**                      | Cluster `local`, status **online**                                                                                 | Stack is healthy                                                                                                                |
| 2   | **Topics**                         | `orders` and `orders.DLQ`, 3 partitions each                                                                       | Created by the app, not auto-created                                                                                            |
| 3   | **Topics → orders → Messages**     | Set **Value Serde** to `SchemaRegistry`. Messages render as `{"orderId":"9001","product":"Laptop","price":1250.5}` | **The Avro proof.** The bytes on disk are binary — it only renders as JSON because the UI resolved the schema from the registry |
| 4   | **Topics → orders.DLQ → Messages** | Only the failed orders                                                                                             | Failures are isolated, not dropped                                                                                              |
| 5   | Click a DLQ message → **Headers**  | `x-failure-class`, `kafka_dlt-original-*`, `kafka_dlt-exception-message`                                           | The same data `GET /api/dlq` returns, read straight off the broker                                                              |
| 6   | **Schema Registry** (sidebar)      | Subject `orders-value` → `Order` with `orderId`, `product`, `price` (float)                                        | Schema stored centrally                                                                                                         |
| 7   | **Consumer Groups**                | `orders-consumer-group` — **STABLE**, **Lag 0**                                                                    | The consumer is keeping up and committing offsets                                                                               |

> If step 3 shows unreadable binary, the Value Serde is set to `String`. Switching it to
> **SchemaRegistry** _is_ the demonstration — do it live.

### Equivalent checks from the terminal

```bash
# Messages currently on each topic
docker exec kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic orders
docker exec kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic orders.DLQ

# Consumer group state and lag - LAG should be 0 when idle
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 \
  --describe --group orders-consumer-group
```

---

## 9. Suggested demo order

1. `docker compose up -d`, then `mvn spring-boot:run -Dspring-boot.run.profiles=consumer,dlq`
2. **§7.1** — schema is registered (Insomnia → Schema Registry)
3. **§7.2** — send two orders, show the running average update, confirm with `GET /api/stats`
4. **§7.4** — `FLAKY-ITEM`: retried twice, then succeeds, counted once
5. **§7.3** — `price: -1`: straight to DLQ, no retries, average unchanged
6. **§7.5** — `ALWAYS-FAIL`: 4 attempts, then DLQ
7. **§7.6** — `GET /api/dlq`: both failures with their reasons
8. **§7.7** — poison pill: consumer survives
9. **§8** — Kafka UI: topics, the Serde switch, DLQ headers, consumer group lag 0
10. Finally, restart with the `producer` profile on to show it running unattended at ~1 order/sec

---

## 10. Configuration

All knobs live in `src/main/resources/application.yml` under `app.*` — topic names, partition count,
produce interval, retry count and backoff timings — plus `server.port` for the API.
