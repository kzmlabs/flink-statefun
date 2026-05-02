# Architecture

Kzmlabs StateFun is a Flink job composed of:

- A **dispatcher operator** that routes incoming messages to the correct function instance, keyed by `(namespace, name, id)`.
- **Stateful function invocations** — either embedded (run inline in the JVM) or remote (called over HTTP).
- **Per-function state primitives** that read and write Flink keyed state through a typed `AddressScopedStorage` interface.
- **Ingress and egress operators** for Kafka, Kinesis, and Flink-native sources/sinks.

```mermaid
flowchart LR
    Kafka[Kafka ingress]:::ingress --> Dispatch[Dispatcher operator]
    Kinesis[Kinesis ingress]:::ingress --> Dispatch
    Dispatch -->|state-keyed message| Func[Function instance]
    Func -->|invoke| Remote[Remote HTTP endpoint]
    Remote -->|response| Func
    Func -->|emit| EgressKafka[Kafka egress]:::egress
    Func -->|emit| EgressKinesis[Kinesis egress]:::egress
    Func -->|state read/write| State[(RocksDB keyed state<br/>checkpointed to S3)]

    classDef ingress fill:#fef3c7,stroke:#f59e0b,color:#92400e
    classDef egress fill:#dbeafe,stroke:#2563eb,color:#1e3a8a
```

## Deeper topics

- [Shading layer](shading.md) — how Protobuf is relocated to avoid version collisions
- [End-to-end tests](e2e-tests.md) — the K8s-native E2E gate
