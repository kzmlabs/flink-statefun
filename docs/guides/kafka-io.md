# Kafka I/O

StateFun functions consume and emit messages through Kafka ingresses and egresses, defined declaratively in `module.yaml`.

## Ingress

```yaml
kind: io.statefun.kafka.v1/ingress
spec:
  id: example/orders
  address: kafka.svc:9092
  consumerGroupId: example-orders
  startupPosition:
    type: latest
  topics:
    - topic: example.orders
      valueType: example/Order
      targets:
        - example/order-handler
```

Each topic entry maps inbound records to a target function namespace+name. The `valueType` declares how StateFun decodes the record value — typically a custom Protobuf type registered in your SDK code.

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

Functions emit to a Kafka egress via `KafkaEgressMessage.builder(...)`. The runtime uses Flink transactions to deliver messages exactly once when paired with a transactional Kafka client and exactly-once Flink checkpointing.

## At-least-once delivery

If the producer doesn't need transactional guarantees, configure `deliverySemantic.type: at-least-once`. Throughput is higher, duplicates can occur on failure recovery.

## Patterns

### Routing by message type

`TargetFunctions.fromPatternString` accepts `<namespace>/<name>` or `<namespace>/*`. Comma-lists and wildcard namespaces are not supported. Use one entry per namespace if you need to target multiple.

### Multi-topic ingresses

A single `kafka.v1/ingress` can declare multiple `topics:` entries. Each entry has its own valueType + targets routing.

### Custom types

The default `io.statefun.types/string` works for simple cases. For typed Protobuf payloads, register `SimpleType.simpleImmutableTypeFrom(...)` in your SDK and reference it as `valueType` — works more reliably than the generic string codec for binary content.

## See also

- [Kinesis I/O](kinesis-io.md) — same model, AWS Kinesis transport
- [Kubernetes deployment](k8s-deployment.md) — wiring the ingress/egress in production
