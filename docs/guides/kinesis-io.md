# Kinesis I/O

Kzmlabs StateFun supports AWS Kinesis Data Streams as both ingress and egress transports, restored on Flink 2.x's `KinesisStreamsSource` and `KinesisStreamsSink`.

## Ingress

The Kinesis ingress is configured in `module.yaml` via the `RoutableKinesisIngressSpec`. Routing is keyed by **stream ARN**, not the short name — a Flink 2.x change from the 1.x source.

```yaml
kind: io.statefun.kinesis.v1/ingress
spec:
  id: example/orders
  awsRegion:
    type: specific
    id: us-east-1
  awsCredentials:
    type: profile
    profile: default
  startupPosition:
    type: at-timestamp
    timestamp: "2026-04-23T00:00:00.000Z"
  streams:
    - stream: orders-stream
      streamArn: arn:aws:kinesis:us-east-1:123456789012:stream/orders-stream
      valueType: example/Order
      targets:
        - example/order-handler
```

Notes:

- **`streams:` array is required** even when `streamArn` is set. The binder re-keys the routing map by ARN, but `valueType` and `targets` still come from the entry.
- **`streamArn` is the source's `setStreamArn()` argument**; the short `stream` field is preserved for symmetry with the egress.
- **`startupPosition.timestamp`** is an ISO-8601 string (`yyyy-MM-dd'T'HH:mm:ss.SSSXXX`), not a numeric epoch.

## Egress

```yaml
kind: io.statefun.kinesis.v1/egress
spec:
  id: example/notifications
  awsRegion:
    type: specific
    id: us-east-1
  awsCredentials:
    type: profile
    profile: default
  streamName: notifications-stream
```

The egress uses Flink 2.x's `KinesisStreamsSink.setStreamName()`, which takes the **short stream name** (not ARN). `streamName` is required.

## LocalStack development

For local testing, point at LocalStack via a custom endpoint:

```yaml
awsRegion:
  type: custom-endpoint
  endpoint: http://localstack.svc:4566
  id: us-east-1
awsCredentials:
  type: basic
  accessKeyId: test
  secretAccessKey: test
```

`AwsRegion.CustomEndpointAwsRegion` accepts both `http://` and `https://` URIs (the previous HTTPS-only constraint is relaxed for LocalStack development).

## Routing details

The Flink 2.x `KinesisStreamsSource` invokes `KinesisDeserializationSchema.deserialize(record, stream, shardId, collector)` with the **stream ARN** as the `stream` argument. `RoutableKinesisIngressDeserializer` relies on this to look up the routing entry by ARN — match the ARN-keyed map. If you bring a custom deserializer, expect the ARN here, not the short name.

## See also

- [Kafka I/O](kafka-io.md) — same routing model, Kafka transport
- [Architecture / E2E tests](../architecture/e2e-tests.md) — `StateFunKinesisE2E` exercises this pipeline against LocalStack
- [`RoutableKinesisIngressSpec`](https://github.com/kzmlabs/flink-statefun/blob/release/statefun-kinesis-io/src/main/java/org/apache/flink/statefun/sdk/kinesis/ingress/RoutableKinesisIngressSpec.java) — Jackson deserializer reference
