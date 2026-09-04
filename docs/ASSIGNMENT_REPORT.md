# Kafka-Based Order Processing System

**Student name:** [Enter your name]
**Student ID:** [Enter your student ID]
**Module:** Big Data
**Assignment:** Chapter 3 - Kafka Order Processing

## 1. Introduction

This project implements a Kafka-based system that produces and consumes order
messages. Every message follows the supplied Avro schema. The consumer maintains
real-time price averages and provides separate handling for recoverable and
permanent processing failures.

The implementation uses Python 3.13, the Apache Kafka 4.3.1 native
local-development image in KRaft mode, `confluent-kafka` as the Kafka client,
and `fastavro` for binary Avro encoding. Docker Compose makes the complete
environment repeatable on another computer.

## 2. Objectives

The implemented objectives are:

1. Produce randomized order messages to Kafka.
2. Serialize and deserialize every order using Avro.
3. Consume orders continuously and calculate running averages.
4. Retry temporary processing failures with exponential backoff.
5. Move permanently failed messages to a Dead Letter Queue.
6. Provide a repeatable live demonstration and a version-controlled repository.

## 3. Order message definition

The file `order.avsc` defines an Avro record named `Order` in the namespace
`lk.assignment.orders`.

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "lk.assignment.orders",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "product", "type": "string"},
    {"name": "price", "type": "float"}
  ]
}
```

The producer validates the record before encoding it. Empty identifiers,
unknown fields, invalid price types, and negative prices are rejected.

## 4. System architecture

The system contains three Kafka topics:

- `orders` receives new Avro order messages.
- `orders.retry` receives temporarily failed orders for another attempt.
- `orders.dlq` stores permanently failed orders and orders whose retries are
  exhausted.

The producer sends messages to `orders`, using the `orderId` as the Kafka key.
The consumer subscribes to both `orders` and `orders.retry`. A successfully
processed order updates the aggregate state. A temporary failure is republished
to the retry topic with an incremented attempt header. A permanent failure is
published to the DLQ.

The original Avro order is preserved as the value during retry and DLQ routing.
Operational information is carried in Kafka headers, keeping the required Avro
schema unchanged.

## 5. Real-time aggregation

The consumer maintains:

- total price and count for all successfully processed orders;
- total price and count for each product;
- a set of processed order IDs for duplicate protection.

After each successful order, the global average is calculated as:

```text
overall running average = sum of processed prices / processed order count
```

The same calculation is performed independently for the current product. The
state is written atomically to `data/averages.json`, allowing the consumer to
continue after a restart without losing its aggregates.

## 6. Failure handling

The demonstration uses deterministic product names so both paths can be shown
on demand.

### 6.1 Temporary failures

An order whose product is `TEMPORARY_FAILURE` fails on attempts 0 and 1. Each
failure is sent to `orders.retry` after exponential backoff. It succeeds on
attempt 2 and then updates the averages.

The initial backoff is one second. The delay is calculated as:

```text
delay = initial backoff * 2 ^ current attempt
```

If the number of retries reaches `MAX_RETRIES`, the consumer sends the order to
the DLQ with `failure-type=retries-exhausted`.

### 6.2 Permanent failures

An order whose product is `PERMANENT_FAILURE` is known not to recover. It is
therefore sent directly to the DLQ with `failure-type=permanent`, avoiding
unnecessary retries.

### 6.3 Invalid data

Unreadable Avro payloads and invalid retry metadata are also routed to the DLQ.
This prevents a poison message from stopping the consumer loop.

## 7. Delivery and offset behavior

The producer enables idempotent publishing and requests acknowledgement from all
available replicas. The consumer turns off automatic commits. It commits a
Kafka offset only after one of these outcomes:

1. the order has been processed successfully;
2. the order has been acknowledged on the retry topic; or
3. the order has been acknowledged on the DLQ topic.

If publication fails, the source offset is not committed and the process exits,
allowing Docker to restart it. Persistent order IDs protect the average from a
redelivered successful order.

## 8. Testing

The automated test suite verifies:

- binary Avro serialization and deserialization;
- rejection of invalid order records;
- global and per-product average calculations;
- aggregate persistence and duplicate handling;
- temporary failures followed by success;
- permanent failures on every attempt;
- Kafka retry-header encoding and validation;
- inclusion of both failure types in a demonstration batch.

Run the test suite with `python -m pytest`.

## 9. Demonstration results

A demonstration batch contains ten messages: eight normal orders, one temporary
failure, and one permanent failure. The expected final behavior is:

- eight normal orders are processed immediately;
- the temporary order is retried twice and then processed;
- the permanent order appears in `orders.dlq`;
- the final successful aggregate count increases by nine;
- the DLQ viewer decodes one failed Avro order and displays its metadata.

Exact averages vary only if the producer seed or order count is changed.

## 10. Conclusion

The implementation fulfills the assignment by combining Kafka event transport,
Avro data contracts, continuous aggregation, controlled retries, and DLQ
isolation. The Docker environment, test suite, documentation, and Git history
make the system reproducible and ready for a live assessment.

## References

1. Apache Kafka. “Downloads.” https://kafka.apache.org/community/downloads/
2. Apache Kafka. “Docker Examples.” https://github.com/apache/kafka/tree/trunk/docker/examples
3. Confluent. “Python Client for Apache Kafka.” https://pypi.org/project/confluent-kafka/
4. fastavro project. https://pypi.org/project/fastavro/
