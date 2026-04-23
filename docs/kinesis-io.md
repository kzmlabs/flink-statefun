# Kinesis I/O — `module.yaml` reference

StateFun's Kinesis ingress/egress is declared via YAML component kinds
`io.statefun.kinesis.v1/ingress` and `io.statefun.kinesis.v1/egress`, parsed
by the binders in `statefun-flink-io-bundle/.../flink/io/kinesis/binders/`.

Runtime: Flink 2.x `KinesisStreamsSource` / `KinesisStreamsSink` backed by
`org.apache.flink:flink-connector-aws-kinesis-streams:6.0.0-2.0`.

## Ingress

```yaml
kind: io.statefun.kinesis.v1/ingress                             # typename
spec:                                                            # object
  id: com.foo.bar/my-ingress                                     # typename
  awsRegion:                                                     # object, optional
    type: specific                                               # string
    id: us-west-2                                                # string
  awsCredentials:                                                # object, optional
    type: basic                                                  # string
    accessKeyId: my_access_key_id                                # string
    secretAccessKey: my_secret_access_key                        # string
  startupPosition:                                               # object, optional
    type: earliest                                               # string
  streamArn: arn:aws:kinesis:us-east-1:000000000000:stream/events # string
  streams:                                                       # array, required
    - stream: events                                             # string
      valueType: com.foo.bar/my-type-1                           # typename
      targets:                                                   # array
        - com.mycomp.foo/function-1                              # typename
        - ...
    - ...
  clientConfigProperties:                                        # array, optional
    - SocketTimeout: 9999                                        # string
    - MaxConnections: 15
    - ...
```

**Notes**

- `streamArn` (Flink 2.x source API requires an ARN). For each ARN there
  must be exactly one entry under `streams:` to supply `valueType` and
  `targets`; the `stream:` field in that entry is a display name — the
  binder routes by ARN at runtime.
- Legacy path: omit `streamArn` and list one or more streams under
  `streams:` by name. Requires the SDK to derive an ARN; unsupported on
  Flink 2.x — prefer `streamArn`.

### `awsRegion` options

| `type`            | additional fields                                                 |
|-------------------|-------------------------------------------------------------------|
| `default`         | — (uses AWS default provider chain)                               |
| `specific`        | `id: <aws region id>` (e.g. `us-east-1`)                          |
| `custom-endpoint` | `endpoint: <url>` + `id: <region id>` (for LocalStack, MinIO, etc.) |

### `awsCredentials` options

| `type`      | additional fields                                             |
|-------------|---------------------------------------------------------------|
| `default`   | — (uses AWS default provider chain)                           |
| `basic`     | `accessKeyId: <...>` + `secretAccessKey: <...>`               |
| `profile`   | `profileName: <...>` + optional `profilePath: <...>`          |

### `startupPosition` options

| `type`      | additional fields                    | connector equivalent |
|-------------|--------------------------------------|----------------------|
| `latest`    | —                                    | `LATEST`             |
| `earliest`  | —                                    | `TRIM_HORIZON`       |
| `date`      | `date: yyyy-MM-dd HH:mm:ss.SSS Z`    | `AT_TIMESTAMP`       |

## Egress

```yaml
kind: io.statefun.kinesis.v1/egress                              # typename
spec:                                                            # object
  id: com.foo.bar/my-egress                                      # typename
  streamName: my-stream                                          # string, required
  awsRegion:                                                     # object, optional
    type: specific
    id: us-west-2
  awsCredentials:                                                # object, optional
    type: basic
    accessKeyId: my_access_key_id
    secretAccessKey: my_secret_access_key
  maxOutstandingRecords: 9999                                    # int, optional
  clientConfigProperties:                                        # array, optional
    - SocketTimeout: 9999
    - MaxConnections: 15
    - ...
```

**Notes**

- `streamName` is required — the Flink 2.x `KinesisStreamsSink` is
  pre-bound to one stream per sink instance. `EgressRecord.getStream()`
  from the SDK is **ignored** at runtime when the sink is pre-bound.

## LocalStack example (development & testing)

LocalStack exposes AWS APIs on plain HTTP at `http://localstack:4566`
(in-cluster) or `http://localhost:4566` (port-forwarded).

```yaml
awsRegion:
  type: custom-endpoint
  endpoint: http://localstack:4566
  id: us-east-1
awsCredentials:
  type: basic
  accessKeyId: test
  secretAccessKey: test
```

The SDK's `AwsRegion.CustomEndpointAwsRegion` accepts `http://` and
`https://`; schemeless and other protocols are rejected.

## See also

- `statefun-k8s-native-e2e/src/test/resources/k8s/module-configmap.yaml` —
  working ingress + egress example against LocalStack.
- `RoutableKinesisIngressSpec` / `GenericKinesisEgressSpec` — Jackson
  deserializer definitions.
