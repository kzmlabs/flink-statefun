---
title: Kinesis I/O
description: AWS Kinesis ingress and egress configuration for Kzmlabs StateFun on Flink 2.x — ARN-keyed routing, LocalStack development, IAM credential modes.
---

# Kinesis I/O

> AWS Kinesis Data Streams as ingress and egress, restored on Flink 2.x's `KinesisStreamsSource` and `KinesisStreamsSink`. Same routing model as Kafka, just AWS-specific configuration.

## Ingress

The Kinesis ingress is configured in `module.yaml` via `RoutableKinesisIngressSpec`. Routing is keyed by **stream ARN**, not the short name — a Flink 2.x change from the 1.x source.

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

!!! note "ARN required"

    The `streams:` array is required even when `streamArn` is set. The binder re-keys the routing map by ARN, but `valueType` and `targets` still come from the entry. The short `stream` field is preserved for symmetry with the egress and for human readability.

### Startup position

```yaml
startupPosition:
  type: at-timestamp
  timestamp: "2026-04-23T00:00:00.000Z"     # ISO-8601 string
```

| `startupPosition.type` | Meaning |
|---|---|
| `latest` | Consume from the latest record on each shard |
| `trim-horizon` | Consume from the oldest available record |
| `at-timestamp` | Consume from records published at or after `timestamp` |

`timestamp` is **always an ISO-8601 string** (`yyyy-MM-dd'T'HH:mm:ss.SSSXXX`), not a numeric epoch.

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

## AWS credential modes

=== "Profile (default)"

    ```yaml
    awsCredentials:
      type: profile
      profile: default
    ```

    Uses the default AWS SDK credential chain — `~/.aws/credentials`, env vars, EC2/EKS instance profile.

=== "Static keys"

    ```yaml
    awsCredentials:
      type: basic
      accessKeyId: AKIAIOSFODNN7EXAMPLE
      secretAccessKey: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    ```

    For dev/test only. Don't commit these to a repo.

=== "Custom"

    ```yaml
    awsCredentials:
      type: custom
      className: com.example.MyCredentialProvider
    ```

    Implement an `AwsCredentialsProviderFactory`; reference it by FQN. For SSO, IAM Roles Anywhere, or other proprietary chains.

## LocalStack development

For local testing without AWS credentials, point at LocalStack via a custom endpoint:

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

`AwsRegion.CustomEndpointAwsRegion` accepts both `http://` and `https://` URIs (the previous HTTPS-only constraint was relaxed for LocalStack development).

The Kzmlabs E2E gate uses LocalStack as its Kinesis backend — see [`StateFunKinesisE2E`](../architecture/e2e-tests.md) for a working reference.

## Routing details

The Flink 2.x `KinesisStreamsSource` invokes `KinesisDeserializationSchema.deserialize(record, stream, shardId, collector)` with the **stream ARN** as the `stream` argument. `RoutableKinesisIngressDeserializer` relies on this to look up the routing entry by ARN.

If you bring a custom deserializer, expect the ARN here, not the short name — this is a Flink 2.x change from the 1.x consumer.

## Next steps

<div class="grid cards" markdown>

- :material-server-network:{ .lg .middle } &nbsp; **[Kafka I/O](kafka-io.md)** — same routing model, Kafka transport.
- :material-test-tube:{ .lg .middle } &nbsp; **[E2E tests](../architecture/e2e-tests.md)** — `StateFunKinesisE2E` exercises this exact pipeline against LocalStack.
- :material-kubernetes:{ .lg .middle } &nbsp; **[Kubernetes deployment](k8s-deployment.md)** — wiring Kinesis I/O in production with IAM Roles for Service Accounts.

</div>
