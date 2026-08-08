---
title: Kafka I/O
description: Apache Kafka ingress and egress configuration for StateFun Actors on Apache Flink - exactly-once delivery, multi-topic ingresses, custom Protobuf types, routing patterns.
---

# Kafka I/O

> StateFun functions consume and emit through Kafka via declarative `module.yaml` specs. Exactly-once when paired with transactional Kafka and Flink checkpointing.

## Ingress

```yaml
kind: io.statefun.kafka.v1/ingress
spec:
  id: example/orders
  address: kafka.svc:9092
  consumerGroupId: example-orders
  startupPosition:
    type: latest                   # or earliest, group-offsets, specific-offsets
  topics:
    - topic: example.orders
      valueType: example/Order      # registered SDK type
      targets:
        - example/order-handler
```

Each entry under `topics:` maps inbound records to a target function namespace + name. The `valueType` declares how StateFun decodes the record value - typically a Protobuf type registered in your SDK code.

### Startup position

| `startupPosition.type` | Meaning |
|---|---|
| `latest` | Consume only records produced after the consumer starts |
| `earliest` | Consume from the beginning of each partition |
| `group-offsets` | Resume from the consumer group's last committed offset (default for restarts) |
| `specific-offsets` | Pin specific partition offsets (advanced) |
| `at-timestamp` | Resume from records produced at or after a wall-clock time |

### Startup-from-timestamp example

```yaml
startupPosition:
  type: at-timestamp
  timestamp: "2026-04-23T00:00:00.000Z"     # ISO-8601
```

### Invalid records

The routable ingress requires a UTF-8 key and a non-null value on every record: the key is the target function instance id, and a null value (tombstone) has no meaning to a function. `invalidRecordHandling` decides what happens to records that violate this - as an ingress-level default, with a per-topic override that replaces it wholesale:

```yaml
kind: io.statefun.kafka.v1/ingress
spec:
  invalidRecordHandling:
    type: skip          # default when omitted
    logLevel: warn      # skip only: debug, info, warn (default) or error
  topics:
    - topic: example.orders
      invalidRecordHandling:
        type: fail      # per-topic override: strict contract for this topic
      ...
```

| `type` | Behavior |
|---|---|
| `skip` (default) | The record is dropped and the job keeps running. Each skipped record is logged individually on the TaskManager and the [invalid-record counters](metrics.md) increment. |
| `fail` | The job fails on the first invalid record, with the record coordinates in the exception. This is the strict pre-3.5 behavior. |

Skip logging is one line per record, at `logLevel`, with no rate limiting:

```
Skipping invalid record: defect [NULL_VALUE], topic [example.orders], partition [0], offset [42], timestamp [1690000000123], key [order-17], value size [-1]
```

`defect` is `NULL_KEY` or `NULL_VALUE`. A null key prints as `key [null]`; a tombstone prints as `value size [-1]`. An empty key is not invalid: it is a legal address that routes to the function instance with id `""`.

Alert rules for the counters: [Alerting](alerting.md).

!!! warning "Behavior change in 3.5"

    Previously an invalid record crashed the whole job unconditionally. `skip` is the new default. Teams alerting on job restarts as their bad-data signal should alert on `numInvalidRecordsSkipped` instead, or pin `type: fail`.

## Egress

```yaml
kind: io.statefun.kafka.v1/egress
spec:
  id: example/notifications
  address: kafka.svc:9092
  deliverySemantic:
    type: exactly-once
    transactionTimeoutMillis: 60000
```

Functions emit to a Kafka egress via the SDK:

```java
KafkaEgressMessage outbound = KafkaEgressMessage.forEgress(
        TypeName.typeNameFromString("example/notifications"))
    .withTopic("example.notifications")
    .withKey(orderId)
    .withValue(notificationPayload)
    .build();

ctx.send(outbound);
```

The runtime uses Flink transactions to deliver exactly once when paired with a transactional Kafka client and exactly-once Flink checkpointing.

### At-least-once vs exactly-once

| `deliverySemantic.type` | Trade-off |
|---|---|
| `exactly-once` | Strongest guarantee. Slightly higher producer latency due to transactions. Default for production. |
| `at-least-once` | Higher throughput. Duplicates possible on JM failover (the runtime replays the last checkpoint window). |

!!! warning "Transaction timeout"

    For `exactly-once`, set `transactionTimeoutMillis` higher than your Flink checkpoint interval, but lower than the Kafka broker's `transaction.max.timeout.ms` (default 15 min). 60 s is a good starting point for sub-minute checkpoint intervals.

## Record headers

Kafka record headers travel in both directions: functions read the headers of the record that
triggered them via `Message#headers()` and attach headers to egress records via the
`KafkaEgressMessage` builder. Ingress header forwarding is opt-in per topic through the
`forwardHeaders` spec property (default `false`, settable at ingress level with per-topic
overrides). Semantics match Kafka's own - duplicate keys, ordering, and the null-vs-empty value
distinction are all preserved, and header operations never throw on null input.

See the dedicated guide: **[Kafka record headers](kafka-headers.md)**.

## Patterns

### Routing to multiple namespaces

`TargetFunctions.fromPatternString` accepts `<namespace>/<name>` or `<namespace>/*`. Comma-lists and wildcard namespaces (`*/foo`) are not supported. To target multiple namespaces, declare one entry per namespace:

```yaml
topics:
  - topic: orders.events
    valueType: example/OrderEvent
    targets:
      - orders/order-handler
      - audit/order-audit
      - billing/order-billing
```

### Multi-topic ingresses

A single `kafka.v1/ingress` can declare multiple `topics:` entries; each has its own `valueType` and `targets`:

```yaml
topics:
  - topic: orders.events
    valueType: example/OrderEvent
    targets: [orders/handler]

  - topic: shipments.events
    valueType: example/ShipmentEvent
    targets: [shipments/handler]
```

### Custom Protobuf types

The default `io.statefun.types/string` works for simple cases. For typed binary payloads, register `SimpleType.simpleImmutableTypeFrom(...)` in your SDK and reference it as `valueType`:

```java
public static final Type<Order> ORDER_TYPE =
    SimpleType.simpleImmutableTypeFrom(
        TypeName.typeNameFromString("example/Order"),
        Order::toByteArray,
        Order::parseFrom);
```

```yaml
valueType: example/Order
```

Works more reliably than the generic string codec for binary content.

## Next steps

<div class="grid cards" markdown>

- :material-aws:{ .lg .middle } &nbsp; **[Kinesis I/O](kinesis-io.md)** - same routing model, AWS Kinesis transport.
- :material-kubernetes:{ .lg .middle } &nbsp; **[Kubernetes deployment](k8s-deployment.md)** - wiring ingress/egress in production.
- :material-graph:{ .lg .middle } &nbsp; **[Architecture overview](../architecture/index.md)** - how the dispatcher routes ingress messages.

</div>
