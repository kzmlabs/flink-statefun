---
title: Logging
description: Structured JSON logs from JobManager and TaskManager pods out of the box. Logstash-standard field names ready for Loki, Elasticsearch, Datadog, CloudWatch.
---

# Logging

> The Kzmlabs image ships **structured JSON logs to stdout by default** from both JobManager and TaskManager pods. No per-deployment `spec.logConfiguration` block needed — drop it, and your aggregator pipeline gets Logstash-standard fields straight away.

## What you get

A single-line JSON record per event, written to stdout. Same shape on JM and TM:

```json
{"@timestamp":"2026-05-17T17:14:32.597Z","@version":"1","message":"Preconfiguration: ","logger_name":"org.apache.flink.runtime.entrypoint.ClusterEntrypoint","thread_name":"main","level":"INFO","level_value":20000}
```

- **Encoder**: `net.logstash.logback.encoder.LogstashEncoder`. Field names: `@timestamp`, `@version`, `level`, `level_value`, `logger_name`, `thread_name`, `message`, `stack_trace`, MDC fields flattened to top level.
- **Stack traces**: `ShortenedThrowableConverter` with `rootCauseFirst=true` — Flink wraps exceptions deeply (task → operator → function → user code), so root-cause-first puts the user-actionable frame on top.
- **No file appender**: pods log to stdout; the container runtime owns rotation and shipping.

Flink stock logger pins are preserved (`org.apache.pekko`, `org.apache.kafka`, `org.apache.hadoop`, `org.apache.zookeeper` at `INFO`; `org.jboss.netty.channel.DefaultChannelPipeline` at `ERROR`), plus one StateFun-specific addition: `com.amazonaws.services.s3.internal.Mimetypes` at `ERROR` to silence the benign AWS SDK v1 startup warning emitted by `flink-s3-fs-presto`.

## How to check what's running

The baked file in the image:

```bash
docker run --rm ghcr.io/kzmlabs/flink-statefun:latest cat /opt/flink/conf/logback-console.xml
```

The file actually used by a running pod (the operator's `spec.logConfiguration` masks the baked one via a ConfigMap mount — this resolves which file is winning):

```bash
kubectl exec <jm-pod> -- cat /opt/flink/conf/logback-console.xml
kubectl exec <tm-pod> -- cat /opt/flink/conf/logback-console.xml
```

The JM and TM mount the same ConfigMap, so they'll always match.

## How to change levels

### Root level via env var

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

Or via JVM opt: `env.java.opts.all: -DROOT_LOG_LEVEL=DEBUG`. Evaluated once at JVM start; pod restart needed to change.

### Full config override via operator

Drop a `spec.logConfiguration.logback-console.xml` block on the `FlinkDeployment`. The Operator mounts a ConfigMap over `${FLINK_HOME}/conf/`, masking the baked file. Use when env-var isn't enough (different encoder, additional appenders, custom per-logger set).

The baked file enables `scan="true" scanPeriod="30 seconds"` — Logback's equivalent of Flink's `monitorInterval=30`. Edits to the operator-mounted ConfigMap are picked up without pod restart.

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

Bump these via `spec.logConfiguration` override (full config) or by adding `<logger>` entries to the operator-mounted ConfigMap (which the running pods pick up within 30s via `scanPeriod`).

## Aggregator compatibility

`LogstashEncoder` defaults drop cleanly into:

| Aggregator | Notes |
|---|---|
| **Elasticsearch / Logstash** | Native — encoder produces the schema Logstash expects. |
| **Loki / Grafana** | Use the JSON parser stage; all fields become labels or detected fields. |
| **Datadog** | Set `service`/`source` via env vars in podTemplate; `level` and `message` map directly. |
| **CloudWatch / OpenObserve** | JSON ingestion native; no parser config needed. |

Existing `level="error"` event counters (e.g., `logback_events_total{level="error"}`) propagate end-to-end unchanged.

## Async logging — when to opt in

The baked config writes **synchronously**, matching Flink upstream. Async is a real optimization at **>5k events/sec/pod**, but brings failure-mode complexity: queue-full behavior, in-flight loss on JVM crash, ordering vs preservation trade-offs, and the need for an explicit `<shutdownHook>` to drain on SIGTERM (Logback 1.4+ no longer auto-registers one).

Container stdout is fast — ~50k lines/sec on modern container runtimes — so sync write is rarely the bottleneck in normal Flink workloads (checkpoint-completion bursts peak around 50–100 events/sec/pod at typical parallelism). If you genuinely need async, override via `spec.logConfiguration` wrapping the `console` appender in `ch.qos.logback.classic.AsyncAppender`.
