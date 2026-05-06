---
title: Kafka egress tuning for high-fanout workloads
description: Practical tuning guide for StateFun Kafka egress when one job emits to many topics — calibrated producer config, parallelism scaling, egress sharding, and a diagnostic flow.
---

# Kafka egress tuning for high-fanout workloads

> A concrete tuning runbook for the case where a StateFun job's Kafka egress fans out to many topics and the producer becomes the job's bottleneck. Written for the Sink V2 connector wiring shipped on this fork (Flink 2.x).

This guide assumes you are using a **single generic `io.statefun.kafka.v1/egress`** declaration with per-message topic routing via `KafkaProducerRecord.topic` — the natural pattern when you have many destination topics. If you have multiple egress declarations, see ["Should I collapse my egresses into one?"](#should-i-collapse-my-egresses-into-one) below.

## Example workload

The numbers throughout this guide are derived from a representative high-fanout topology. Adjust to your case by recomputing the per-batch math (formula at the end).

| Aspect | Value |
|---|---|
| Function binding | Per-entity instance (one function instance per business entity) |
| Ingress topics | 20 |
| Egress topics | 20 |
| Partitions per topic | 12 |
| Job parallelism | 12 |
| Egress declaration | Single `io.statefun.kafka.v1/egress` with per-message topic routing |
| Delivery semantic | `at-least-once` |
| Symptom | Egress is the job's main bottleneck → backpressure on `FunctionGroupOperator` |

**Math at a glance:**

- Ingress: `20 × 12 = 240` partitions consumed across 12 source subtasks → 20 partitions per subtask. Comfortable headroom.
- Egress: 12 Kafka producer instances total (one per subtask), each fanning out to up to **240** `(topic, partition)` batch slots simultaneously.

## Foundational checks before tuning

Tuning a system that's already incorrect just makes the bug faster. Verify these in order.

### Producer record key

In your function code, the `KafkaProducerRecord` proto must carry a `key` that is the natural ordering key of your domain (entity ID, account ID, payment ID, device ID, etc.):

```java
KafkaProducerRecord record = KafkaProducerRecord.newBuilder()
    .setTopic("events.captured")
    .setKey(entityId)                                   // REQUIRED
    .setValueBytes(eventPayload.toByteString())
    .build();
```

Without a stable key, events for the same entity can land in different partitions across redeploys or rebalances. The consumer then sees events out of causal order. **No producer tuning fixes a missing or unstable key.**

### Downstream consumer idempotence

`at-least-once` means duplicate records on retry. Consumers must dedupe.

| Consumer type | Idempotence key | Risk if not idempotent |
|---|---|---|
| Audit log / event store | `event_id` (UUID) | Duplicate rows. Recoverable. |
| Notifications | `event_id` | Duplicate user-visible messages. |
| **Ledger / accounting** | **`event_id` + monotonic `seq_no`** | **Double-booking. Silent until reconciliation.** |
| Fraud / scoring / analytics | `event_id` | Wasted compute; not a correctness bug. |
| External webhooks | depends on remote API | Often the weakest link — dedup at your edge if needed. |

If a critical consumer is not idempotent, two options:

1. **Make it idempotent at storage** (`INSERT … ON CONFLICT DO NOTHING` keyed on `event_id`). Always preferable.
2. **Declare a separate `exactly-once` egress** for that one consumer's topic. Mix semantics in one job — high-volume events on the at-least-once egress, critical-correctness events on the EOS egress. Cost: 2PC overhead on the EOS path only.

### State TTL

Per-key state grows unbounded if not expired. Set TTL on terminal states:

```java
@Persisted
private final PersistedValue<EntityState> state = PersistedValue.of(
    "state",
    EntityState.class,
    Expiration.expireAfterWrite(Duration.ofDays(90)));
```

Unbounded state grows RocksDB → checkpoint duration grows linearly → looks identical to "egress is slow" in the Flink UI but is actually checkpoint-flush blocking. Verify before blaming the producer.

### RocksDB checkpoints

```yaml
state.backend.type: rocksdb
state.backend.incremental: "true"
```

Async snapshots are on by default. Incremental is the difference between checkpointing in seconds vs minutes at high state count.

## Calibrated producer configuration

For a single multi-topic egress fanning out to **240** `(topic, partition)` batch slots per producer:

```yaml
kind: io.statefun.kafka.v1/egress
spec:
  id: app/all-out
  address: kafka:9092
  deliverySemantic: { type: at-least-once }
  properties:
    # === Memory ===
    # Must hold up to 240 simultaneous open batches plus headroom.
    # 128 MB / 240 ≈ 533 KB per batch worst-case → comfortable headroom.
    - buffer.memory: "134217728"               # 128 MB

    # === Batching ===
    # Calibrated DOWN from naive single-topic recommendation.
    # 128 KB × 240 batches = 30 MB worst-case → fits in 128 MB.
    # Larger batch.size with this fanout overflows buffer.memory.
    - batch.size: "131072"                     # 128 KB

    # Lets per-(topic, partition) batches accumulate before send.
    # With 20 topics, individual batches fill slowly; linger.ms is doing
    # real work here. Cap: keep < checkpoint.interval / 4 to avoid
    # flush-on-barrier stalls.
    - linger.ms: "30"

    # === Compression ===
    # Big payoff with diverse topic payloads. lz4 is fastest with good
    # ratio; zstd is slightly better ratio at higher CPU.
    - compression.type: "lz4"

    # === Pipelining ===
    - max.in.flight.requests.per.connection: "5"

    # === Reliability ===
    # idempotence + acks=all = no duplicates within a single producer epoch
    # AND no data loss on broker leader failover.
    # Note: at-least-once semantic still applies across checkpoint boundaries
    # (a failed task replays from last checkpoint), so consumer dedup remains
    # required. Idempotence handles producer-side retries only.
    - enable.idempotence: "true"
    - acks: "all"
    - retries: "2147483647"

    # === Headroom ===
    - max.request.size: "4194304"              # 4 MB — handles bursty large records
    - request.timeout.ms: "60000"
    - delivery.timeout.ms: "300000"
```

### Why these values aren't the same as single-topic recommendations

The naive advice for a high-throughput Kafka producer is `batch.size = 256–512 KB` plus `buffer.memory = 64 MB`. **That's wrong for fanout this wide:**

- `batch.size` is a **per-batch** cap. With 240 open batches, naive `512 KB × 240 = 122 MB` worst-case — blows past `buffer.memory = 32–64 MB` defaults.
- The producer then blocks on `bufferpool-wait-time` during sends → mailbox stalls → backpressure.
- Solution: smaller `batch.size` (128 KB) × bigger `buffer.memory` (128 MB) → both fit, both effective.
- `linger.ms` does most of the batching work for you when batches don't fill on size alone, which is most of the time across many topics.

### Calibrated ingress configuration

```yaml
kind: io.statefun.kafka.v1/ingress
spec:
  id: app/all-in
  address: kafka:9092
  consumerGroupId: app-group
  startupPosition: { type: groupOffsets }
  topics:
    - topic: ...
  properties:
    - fetch.min.bytes: "131072"                # 128 KB — fewer broker round-trips
    - fetch.max.wait.ms: "200"                 # let bytes accumulate up to 200 ms
    - max.partition.fetch.bytes: "1048576"     # 1 MB default, fine
    - max.poll.records: "2000"                 # bigger poll batches
```

## Parallelism scaling

| Parallelism | Partitions/subtask (ingress) | Producers | Notes |
|---|---|---|---|
| 12 (example) | 20 | 12 | Comfortable. Each producer is busy. |
| **24** | 10 | 24 | **Sweet spot.** Doubles producer Sender threads. |
| 30 | 8 | 30 | Marginal gain over 24. |
| 60 | 4 | 60 | Diminishing returns; per-producer batches starve. |
| 240 | 1 | 240 | Don't. No rebalance slack; tiny batches everywhere. |

```yaml
# FlinkDeployment CR / flink-conf.yaml
parallelism.default: "24"
```

At parallelism `P`, you have `P` Kafka producer instances total. Each producer has a single Sender thread that serializes, compresses, and dispatches all in-flight requests. Doubling `P` doubles the Sender-thread budget. The sweet spot is around 24–30 for a 20-topic fanout. Past that, batching efficiency falls faster than parallelism gains.

## Egress sharding (when Sender thread saturates)

If post-tuning and post-parallelism-scaling, the Kafka client JMX shows `record-send-rate` plateaued and TaskManager CPU on the producer Sender thread is saturated, the bottleneck is the **single Sender thread per producer instance**. Shard the egress into K parallel egresses, each with its own producer per subtask.

### Module YAML (K = 2 example)

```yaml
kind: io.statefun.kafka.v1/egress
spec:
  id: app/all-out-0
  address: kafka:9092
  deliverySemantic: { type: at-least-once }
  properties: [ ... same calibrated tuning as above ... ]
---
kind: io.statefun.kafka.v1/egress
spec:
  id: app/all-out-1
  address: kafka:9092
  deliverySemantic: { type: at-least-once }
  properties: [ ... same ... ]
```

### Function code

```java
private static final int NUM_SHARDS = 2;
private static final List<EgressIdentifier<TypedValue>> SHARDS = List.of(
    EgressIdentifier.of(MyApp.class, "all-out-0", TypedValue.class),
    EgressIdentifier.of(MyApp.class, "all-out-1", TypedValue.class));

private static EgressIdentifier<TypedValue> shardFor(String topic) {
    int shard = Math.floorMod(topic.hashCode(), NUM_SHARDS);
    return SHARDS.get(shard);
}

// At emit time:
ctx.send(shardFor("events.captured"), typedValueFor(record));
```

### Why hash-by-topic preserves ordering

The per-entity ordering invariant says: for entity X, events in topic T must reach the consumer in causal order.

- StateFun's `keyBy(entity_id)` ensures all events for X are processed in order on a single subtask.
- The function emits them in that order.
- All events targeting topic T (regardless of entity) always go to the same shard (deterministic `hash(T)`).
- That shard's producer writes to `(T, partition_for_X)` in arrival order.
- With `enable.idempotence=true`, retries preserve per-partition ordering.

✅ Per-entity per-topic ordering preserved.

### Anti-pattern — sharding by entity ID

```java
// WRONG: do not shard by entity ID
int shard = Math.floorMod(entityId.hashCode(), NUM_SHARDS);
```

This splits events for the same entity across producers, complicating the ordering reasoning and giving up the "all events for entity X in topic T flow through the same producer instance" property. Use `hash(topic)` instead.

### Trade-off

K shards = K producers per subtask = K× Sender-thread budget. But each shard sees only ~1/K of the per-(topic, partition) flow, so batches fill K× more slowly → less batching efficiency. **Sweet spot: K = 2–4.**

Use this only after you've measured Sender-thread saturation. Producer tuning plus parallelism scaling resolve most cases on their own.

## Diagnostic flow

Step through this top-to-bottom under load.

### Where is backpressure?

In the Flink UI → Job → per-vertex `Backpressured (%)`:

| Backpressured vertex | Bottleneck likely is |
|---|---|
| Source | Downstream — function or sink. Tune downstream, not source. |
| `FunctionGroupOperator` | Sink. Apply the levers in this guide. |
| Sink writer | Broker / Kafka cluster. Producer tuning won't fix. |

### What does the function operator look like?

`FunctionGroupOperator` → `Busy (%)` per subtask:

| Observation | Bottleneck |
|---|---|
| All subtasks ~100% busy | Function CPU (state, HTTP). Scale parallelism or remote-fn pods. |
| One subtask ~100%, others idle | **Data skew.** Producer tuning won't fix. |
| All subtasks ~50% busy, backpressured | Mailbox-stalled on `producer.send()`. Producer-side bottleneck. |

### What does the producer JMX show?

Get the producer client JMX from the TaskManager:

```
kafka.producer:type=producer-metrics,client-id=*
```

| Metric | Reading | Meaning | Fix |
|---|---|---|---|
| `bufferpool-wait-time-total` | Rising | `buffer.memory` saturated | Raise `buffer.memory` to 256 MB. |
| `record-send-rate` | Flat under increasing load | Sender thread or broker capped | If TM CPU high → egress sharding. If broker idle low → broker-side. |
| `compression-rate-avg` | Close to 1.0 | Batches too small to compress | Raise `linger.ms`. |
| `request-latency-avg` | Rising over time | Broker is slow | Scale Kafka cluster. No client fix. |
| `record-error-rate` | > 0 | Producer retries or drops | Investigate logs. |
| `batch-size-avg` | < 10 KB | Underbatched | Raise `linger.ms` and/or `batch.size`. |
| `batch-size-avg` | Approaching `batch.size` cap | Well-batched | Tuning is working. |

### What does the broker show?

| Metric | Reading | Meaning |
|---|---|---|
| `RequestHandlerAvgIdlePercent` | < 30% | Broker CPU-bound. Add brokers / partitions. |
| `NetworkProcessorAvgIdlePercent` | < 30% | Broker network-bound. Add brokers / NIC. |
| `BytesInPerSec` per topic | Heavily skewed | Topic-level hotspot. Repartition the hot topic. |
| `UnderReplicatedPartitions` | > 0 | Replication lagging. Cluster health issue. |

### Per-key hotkey check

Run this against your event store over a representative window:

```sql
SELECT key_id, COUNT(*) AS event_count
FROM events
WHERE created_at > NOW() - INTERVAL 24 HOUR
GROUP BY key_id
ORDER BY event_count DESC
LIMIT 100;
```

If the top 1% of keys receive >50% of events, you have key-level skew. Fixes are in the data model, not Kafka:

| Pattern | Fix |
|---|---|
| Test / sandbox traffic to one entity | Route to separate StateFun deployment or shard with synthetic suffix. |
| Webhook retry storm on one ID | Idempotency at ingress + rate-limit per `key_id`. |
| Hierarchical aggregator with high child volume | Bind function to *child ID*, not parent ID. |
| Long-lived entity with sustained event flow | Usually fine — instantaneous QPS still low. |

## Recommended order of attack

1. **Verify producer record `key`** is the entity ID. Foundational ordering correctness.
2. **Verify downstream consumer idempotence** on critical consumers (especially anything that writes a ledger). Foundational correctness.
3. **Verify state TTL + incremental checkpoints**. Eliminates a common false-positive.
4. **Apply the calibrated producer tuning**. YAML change only.
5. **Bump parallelism `12 → 24`** (or proportional for your topology). Single config flip.
6. **Re-measure** backpressure + producer JMX. Most pipelines stop here.
7. **If `bufferpool-wait-time-total` still rising** → raise `buffer.memory` to 256 MB.
8. **If `record-send-rate` plateaus with high TM CPU on Sender thread** → egress sharding (K = 2).
9. **If broker-side metrics show capacity issues** → scale Kafka, not StateFun.

## What not to do

- Don't increase `batch.size` past 128 KB without raising `buffer.memory` proportionally — `batch.size × open_batch_slots` must fit in `buffer.memory`.
- Don't shard by entity ID — complicates ordering reasoning and gives up locality. Use `hash(topic)` if you shard.
- Don't switch to exactly-once "for safety" if at-least-once with idempotent consumers is acceptable — EOS roughly halves throughput and adds checkpoint-coupled commit latency.
- Don't call `withKafkaProducerPoolSize(N)` — silently no-op on this fork; the new `KafkaSink` (Sink V2) manages its transactional ID space internally.
- Don't enable unaligned checkpoints — `StatefulFunctionsConfigValidator` rejects them. StateFun's feedback-loop topology is incompatible.
- Don't go past parallelism 30× the partition-per-topic count — diminishing returns are real.

## Should I collapse my egresses into one?

If you have many separate `io.statefun.kafka.v1/egress` declarations, one per destination topic, **collapse them into a single generic egress with per-message topic routing**. Each separate declaration creates its own `KafkaSink` operator, its own producer per subtask, and its own batch buffers — at parallelism `P` with N declarations you run `N × P` Kafka producers, each seeing only 1/N of the traffic. Tiny batches, N× the network connections, N× the heap.

The generic egress accepts a `KafkaProducerRecord` proto whose `topic` field is set per-message. One sink, one producer per subtask, all topics share the same batch window. The function emits with `topic`, `key`, and `value` at send time. This is the structural prerequisite for everything else in this guide.

## Re-tuning later

When QPS or topic count changes, re-derive `buffer.memory` and `batch.size` from:

```
open_batch_slots_worst_case  =  topics × partitions_per_topic     (per producer instance)

batch.size × open_batch_slots_worst_case  ≤  buffer.memory × 0.7  (leave 30% headroom)
```

For the example topology: `240 × 128 KB = 30 MB ≤ 128 MB × 0.7 = 90 MB` ✅.

If topic count doubles to 40, `batch.size = 128 KB × 480 = 60 MB` — still fits. If you also raise `batch.size` to 256 KB: `256 KB × 480 = 120 MB > 90 MB` → `buffer.memory` must rise to 256 MB.

Always verify under load with `bufferpool-wait-time-total` before declaring tuning done.

## See also

- [Kafka I/O](kafka-io.md) — full ingress / egress configuration reference
- [Kubernetes deployment](k8s-deployment.md) — `FlinkDeployment` CR setup, parallelism config
- [Differences from Apache StateFun](../upstream-vs-kzm.md) — Source V2 / Sink V2 migration notes, savepoint compatibility
