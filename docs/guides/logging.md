---
title: Logging
description: Structured JSON logs out of the box, runtime per-logger control via JMX, env-var root level, and full operator override. ECS-aligned field names ready for Loki, Elasticsearch, Datadog.
---

# Logging

> The Kzmlabs image ships **structured JSON logs to stdout by default** via Logback + `LogstashEncoder`. No per-deployment `spec.logConfiguration` block needed — drop it, and your aggregator pipeline gets ECS-aligned fields straight away.

## What you get

A single-line JSON record per event, written to stdout (real output captured from a `jobmanager` container):

```json
{"@timestamp":"2026-05-17T17:14:32.597Z","@version":"1","message":"Preconfiguration: ","logger_name":"org.apache.flink.runtime.entrypoint.ClusterEntrypoint","thread_name":"main","level":"INFO","level_value":20000}
```

- **Encoder**: `net.logstash.logback.encoder.LogstashEncoder` (Logstash defaults: `@timestamp`, `@version`, `level`, `level_value`, `logger_name`, `thread_name`, `message`, `stack_trace`, MDC fields flattened to top level).
- **Stack traces**: `ShortenedThrowableConverter` with `rootCauseFirst=true` — Flink wraps exceptions ~3× deeper than typical apps (task → operator → function → user code), so root-cause-first puts the user-actionable line on top.
- **File appender dropped**: Pods log to stdout; the container runtime owns rotation and shipping.

The verbatim Flink stock logger pins are preserved (`org.apache.pekko`, `org.apache.kafka`, `org.apache.hadoop`, `org.apache.zookeeper` at `INFO`; `org.jboss.netty.channel.DefaultChannelPipeline` at `ERROR`), plus one StateFun-specific addition: `com.amazonaws.services.s3.internal.Mimetypes` at `ERROR` to silence the benign AWS SDK v1 startup warning emitted by `flink-s3-fs-presto`.

## How to check what's running

The baked file in the image:

```bash
docker run --rm ghcr.io/kzmlabs/flink-statefun:latest cat /opt/flink/conf/logback-console.xml
```

The file actually used by a running pod (the operator's `spec.logConfiguration` masks the baked one via a ConfigMap mount — this resolves which file is winning):

```bash
kubectl exec <jm-pod> -- cat /opt/flink/conf/logback-console.xml
```

## How to change levels

Three mechanisms, ordered least → most invasive.

### 1. Runtime per-logger change via JMX — no restart, ephemeral

Best for in-incident `DEBUG` bumps. Reverts on pod restart.

The baked config includes `<jmxConfigurator/>`, which exposes the `ch.qos.logback.classic:Name=default,Type=ch.qos.logback.classic.jmx.JMXConfigurator` MBean. You will need a JMX-enabled JVM — add to `env.java.opts.all`:

```yaml
env.java.opts.all: >-
  -Dcom.sun.management.jmxremote
  -Dcom.sun.management.jmxremote.port=9010
  -Dcom.sun.management.jmxremote.local.only=false
  -Dcom.sun.management.jmxremote.authenticate=false
  -Dcom.sun.management.jmxremote.ssl=false
  -Djava.rmi.server.hostname=127.0.0.1
```

!!! warning "JMX security note"
    The flags above disable authentication and TLS — only safe behind `kubectl port-forward`. **Never expose JMX through a Service.** For production with authentication, use `-Dcom.sun.management.jmxremote.password.file` + `-Dcom.sun.management.jmxremote.access.file` per the [Oracle JMX guide](https://docs.oracle.com/en/java/javase/21/management/monitoring-and-management-using-jmx-technology.html).

Then port-forward and call the MBean:

```bash
kubectl port-forward pod/<jm-pod> 9010:9010
echo 'run -b ch.qos.logback.classic:Name=default,Type=ch.qos.logback.classic.jmx.JMXConfigurator \
  setLoggerLevel org.apache.flink.runtime.checkpoint DEBUG' \
  | java -jar jmxterm-1.0.4-uber.jar -l localhost:9010 -n
```

MBean ops: `getLoggerList`, `getLoggerLevel`, `getLoggerEffectiveLevel`, `setLoggerLevel(name, level)`, `reloadDefaultConfiguration`. Pass `null` as level to clear an override.

### 2. Root level change via env var — pod restart, persistent until env var changes

The baked config reads `${ROOT_LOG_LEVEL:-INFO}`. Override per deployment:

```yaml
spec:
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          env:
            - name: ROOT_LOG_LEVEL
              value: DEBUG
```

Or via JVM opt: `env.java.opts.all: -DROOT_LOG_LEVEL=DEBUG`. Evaluated once at JVM start; restart needed to change.

### 3. Full config override via operator — pod restart, fully customized

Drop a `spec.logConfiguration.logback-console.xml` block on the `FlinkDeployment`. The Operator mounts a ConfigMap over `${FLINK_HOME}/conf/`, masking the baked file. Use when env-var + JMX aren't enough (different encoder, additional appenders, custom logger set).

The baked file also enables `scan="true" scanPeriod="30 seconds"` — Logback's equivalent of Flink's `monitorInterval=30`. Edits to the operator-mounted file are picked up without restart.

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

## Async logging — when to opt in

The baked config writes **synchronously**, matching Flink upstream. Async is a real optimization at **>5k events/sec/pod**, but brings failure-mode complexity: queue-full behavior, in-flight loss on JVM crash, ordering vs preservation trade-offs, and the need for an explicit `<shutdownHook>` to drain on SIGTERM (Logback 1.4+ no longer auto-registers one).

Container stdout is fast — kernel pipe → container runtime → kubectl logs at ~50k lines/sec on modern runtimes — so sync write is rarely the bottleneck in normal Flink workloads (checkpoint-completion bursts peak around 50–100 events/sec/pod at typical parallelism). If you genuinely need async, override via `spec.logConfiguration` wrapping the `console` appender in `ch.qos.logback.classic.AsyncAppender`.

## Field schema and aggregator compatibility

`LogstashEncoder` defaults match the Elastic Common Schema (ECS) closely enough to drop into:

| Aggregator | Notes |
|---|---|
| **Elasticsearch / Logstash** | Native — encoder produces the schema Logstash expects. |
| **Loki / Grafana** | Use the JSON parser stage; all fields become labels or detected fields. |
| **Datadog** | Set `service`/`source` via env vars in podTemplate; `level` and `message` map directly. |
| **CloudWatch / OpenObserve** | JSON ingestion native; no parser config needed. |

Existing `level="error"` event counters (e.g., `logback_events_total{level="error"}`) propagate end-to-end unchanged.
