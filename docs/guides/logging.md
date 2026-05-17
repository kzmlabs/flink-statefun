---
title: Logging
description: How JobManager and TaskManager logging is configured in StateFun Actors on Kubernetes — spec.logConfiguration, the Operator ConfigMap overlay, JSON output via LogstashEncoder.
---

# Logging

## How it works in StateFun on K8s

Both JobManager and TaskManager read their Logback config from `${FLINK_HOME}/conf/logback-console.xml`. Under the Flink Kubernetes Operator that file lives in an auto-generated read-only ConfigMap that the Operator mounts over `/opt/flink/conf/` — the mount **replaces the whole directory**, so anything baked into the image at `/opt/flink/conf/*` is masked at runtime.

The Operator builds that ConfigMap from two sources:

1. **`config.yaml`** — derived from `spec.flinkConfiguration`.
2. **`spec.logConfiguration`** — a map of `<filename> → <content>` you provide on the `FlinkDeployment`. The key for Logback is exactly `logback-console.xml`.

If `spec.logConfiguration.logback-console.xml` is not set, the Operator omits the key entirely. Logback then has no config file to read, falls back to `BasicConfigurator`, and emits plain-text WARN+ to stderr. Structured JSON is **opt-in via this block**.

## JSON config used by StateFun E2E

The reference XML lives at [`statefun-e2e-tests/statefun-e2e-k8s-native/src/test/resources/k8s/flink-deployment.yaml`](https://github.com/kzmlabs/flink-statefun/blob/release/statefun-e2e-tests/statefun-e2e-k8s-native/src/test/resources/k8s/flink-deployment.yaml). Copy-paste:

```yaml
spec:
  logConfiguration:
    logback-console.xml: |
      <?xml version="1.0" encoding="UTF-8"?>
      <configuration scan="true" scanPeriod="30 seconds">
          <appender name="console" class="ch.qos.logback.core.ConsoleAppender">
              <encoder class="net.logstash.logback.encoder.LogstashEncoder">
                  <timestampPattern>yyyy-MM-dd'T'HH:mm:ss.SSS'Z'</timestampPattern>
                  <throwableConverter class="net.logstash.logback.stacktrace.ShortenedThrowableConverter">
                      <maxDepthPerThrowable>120</maxDepthPerThrowable>
                      <shortenedClassNameLength>60</shortenedClassNameLength>
                      <exclude>^sun\.reflect\..*\.invoke</exclude>
                      <exclude>^jdk\.internal\.reflect\..*</exclude>
                      <exclude>^java\.lang\.reflect\.Method\.invoke</exclude>
                      <rootCauseFirst>true</rootCauseFirst>
                  </throwableConverter>
              </encoder>
          </appender>
          <root level="${ROOT_LOG_LEVEL:-INFO}">
              <appender-ref ref="console"/>
          </root>
          <logger name="org.apache.pekko"                                level="INFO"/>
          <logger name="org.apache.kafka"                                level="INFO"/>
          <logger name="org.apache.hadoop"                               level="INFO"/>
          <logger name="org.apache.zookeeper"                            level="INFO"/>
          <logger name="org.jboss.netty.channel.DefaultChannelPipeline"  level="ERROR"/>
          <logger name="com.amazonaws.services.s3.internal.Mimetypes"    level="ERROR"/>
      </configuration>
```

What each piece does:

- **`LogstashEncoder`** — single-line JSON per event; Logstash-standard field names (`@timestamp`, `level`, `logger_name`, `thread_name`, `message`, `stack_trace`, MDC fields flattened).
- **`ShortenedThrowableConverter` + `rootCauseFirst=true`** — Flink wraps exceptions deeply (task → operator → function → user code); root-cause-first puts the user-actionable frame on top. The `<exclude>` patterns drop reflection plumbing frames.
- **`scan="true" scanPeriod="30 seconds"`** — Logback's auto-reload. Edits to the operator-mounted ConfigMap are picked up within 30 seconds **without pod restart**.
- **`${ROOT_LOG_LEVEL:-INFO}`** — root level falls back to `INFO` but can be overridden per deployment via env var or `-D` JVM opt (see below).
- **Flink stock logger pins** — verbatim from upstream `flink/conf/logback-console.xml`. Pekko/Kafka/Hadoop/Zookeeper at `INFO`, Netty's noisy pipeline logger at `ERROR`.
- **`com.amazonaws.services.s3.internal.Mimetypes` ERROR** — silences a benign AWS SDK v1 startup warning emitted by `flink-s3-fs-presto`.

Sample emitted record (real JM output):

```json
{"@timestamp":"2026-05-17T17:14:32.597Z","@version":"1","message":"Preconfiguration: ","logger_name":"org.apache.flink.runtime.entrypoint.ClusterEntrypoint","thread_name":"main","level":"INFO","level_value":20000}
```

## Override the root level

| Mechanism | Effect | Scope |
|---|---|---|
| `-e ROOT_LOG_LEVEL=DEBUG` (container env) | Single value, all loggers | Per pod, restart needed |
| `-DROOT_LOG_LEVEL=DEBUG` (JVM opt via `env.java.opts.all`) | Same as above | Per pod, restart needed |
| `<logger>` entries added to `spec.logConfiguration.logback-console.xml` | Per-logger overrides | Per FlinkDeployment, restart needed |
| Edit the operator-mounted ConfigMap directly (`kubectl edit cm flink-config-<deployment>`) | Per-logger overrides | Picked up within 30s via `scan="true"`, **no restart** |

The last option is the safest in-incident path — no pod cycle.

## In-incident DEBUG candidates

| Symptom | Logger to bump |
|---|---|
| Checkpoints failing / slow | `org.apache.flink.runtime.checkpoint.CheckpointCoordinator` |
| HA failover / split brain | `org.apache.flink.runtime.leaderelection` |
| TM crash / restart | `org.apache.flink.runtime.taskmanager` |
| Kafka consumer-group rebalance | `org.apache.kafka.clients.consumer.internals.ConsumerCoordinator` |
| Kafka source lag | `org.apache.flink.connector.kafka.source` |
| StateFun routing / dispatch | `org.apache.flink.statefun.flink.core` |
| K8s client errors | `io.fabric8.kubernetes.client` |
| RocksDB state-backend | `org.apache.flink.contrib.streaming.state` |

## How to verify what the pod actually sees

```bash
kubectl exec <jm-pod> -- cat /opt/flink/conf/logback-console.xml
kubectl exec <tm-pod> -- cat /opt/flink/conf/logback-console.xml
```

If the output is empty or doesn't contain `LogstashEncoder`, the Operator's ConfigMap was generated without the `logback-console.xml` key — re-check `spec.logConfiguration` on the FlinkDeployment.

## Aggregator compatibility

`LogstashEncoder` defaults drop cleanly into Elasticsearch / Logstash (native), Loki / Grafana (JSON parser stage), Datadog (`level`/`message` map directly), and CloudWatch / OpenObserve (JSON ingestion native). Existing `logback_events_total{level="error"}` counters propagate unchanged.

## Async logging — when to opt in

The reference config writes **synchronously**, matching Flink upstream. Sync is fine up to ~5k events/sec/pod; checkpoint-completion bursts on typical workloads peak at 50–100 events/sec/pod. For genuinely high throughput, wrap the `console` appender in `ch.qos.logback.classic.AsyncAppender` and add an explicit `<shutdownHook>` to drain on SIGTERM (Logback 1.4+ no longer auto-registers one).
