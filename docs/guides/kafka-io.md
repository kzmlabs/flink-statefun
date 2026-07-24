---
title: Kafka I/O
description: Apache Kafka ingress and egress configuration for StateFun Actors on Apache Flink — exactly-once delivery, multi-topic ingresses, custom Protobuf types, routing patterns.
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

Each entry under `topics:` maps inbound records to a target function namespace + name. The `valueType` declares how StateFun decodes the record value — typically a Protobuf type registered in your SDK code.

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

## Record headers

Kafka record headers travel in both directions.

**Reading ingress headers** — a function invoked by a record from a Kafka ingress can read the
record's headers through `Message#headers()`. Duplicate keys are allowed and the original order is
preserved; messages that did not originate from Kafka (e.g. function-to-function sends) return an
empty list:

```java
for (MessageHeader header : message.headers()) {
    if (header.key().equals("trace-id")) {
        String traceId = header.valueAsUtf8String();
    }
}
```

**Setting egress headers** — attach headers when building a `KafkaEgressMessage`; the runtime
copies them onto the produced Kafka record:

```java
KafkaEgressMessage.forEgress(TypeName.typeNameFromString("example/notifications"))
    .withTopic("example.notifications")
    .withKey(orderId)
    .withValue(notificationPayload)
    .withUtf8Header("trace-id", traceId)
    .withHeader("payload-hash", hashBytes)
    .build();
```

Header support is additive at the protocol level: remote functions built against older SDKs keep
working unchanged, they simply do not see headers.

### At-least-once vs exactly-once

| `deliverySemantic.type` | Trade-off |
|---|---|
| `exactly-once` | Strongest guarantee. Slightly higher producer latency due to transactions. Default for production. |
| `at-least-once` | Higher throughput. Duplicates possible on JM failover (the runtime replays the last checkpoint window). |

!!! warning "Transaction timeout"

    For `exactly-once`, set `transactionTimeoutMillis` higher than your Flink checkpoint interval, but lower than the Kafka broker's `transaction.max.timeout.ms` (default 15 min). 60 s is a good starting point for sub-minute checkpoint intervals.

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

- :material-aws:{ .lg .middle } &nbsp; **[Kinesis I/O](kinesis-io.md)** — same routing model, AWS Kinesis transport.
- :material-kubernetes:{ .lg .middle } &nbsp; **[Kubernetes deployment](k8s-deployment.md)** — wiring ingress/egress in production.
- :material-graph:{ .lg .middle } &nbsp; **[Architecture overview](../architecture/index.md)** — how the dispatcher routes ingress messages.

</div>
