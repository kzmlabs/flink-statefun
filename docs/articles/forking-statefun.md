---
title: We forked Apache Stateful Functions for Flink 2.x — here's why
description: Apache Stateful Functions has been dormant since October 2024. Here's why we maintained the continuation on Flink 2.x and Java 21, with Kinesis I/O restored.
---

# We forked Apache Stateful Functions for Flink 2.x — here's why

*Published 2026-05-03 · by the kzmlabs maintainers · [Read on dev.to](https://dev.to/okazimirov/we-forked-apache-stateful-functions-for-flink-2x-heres-why-18oj)*

[Apache Stateful Functions][upstream] is one of the quietly powerful frameworks in the Flink ecosystem — durable per-key state, exactly-once messaging, polyglot remote functions, all on top of Apache Flink. It's also been functionally dormant since **October 2024**, and it doesn't run on Flink 2.x.

We needed it on Flink 2.x. So we maintained the continuation: [`kzmlabs/flink-statefun`][our-repo].

This post is the *why* and the *how it's different*. If you're already running upstream and wondering whether to migrate, or you're picking a stateful-actor framework today and trying to understand the landscape, read on.

## What StateFun gives you

If you've never touched Stateful Functions, here's the elevator version. Compare a hand-written keyed Flink job:

```java
public final class CounterFunction
    extends KeyedProcessFunction<String, Event, Result> {

  private static final long serialVersionUID = 1L;

  private static final ValueStateDescriptor<Long> COUNT_DESCRIPTOR =
      new ValueStateDescriptor<>("count", Types.LONG);

  private transient ValueState<Long> count;

  @Override
  public void open(OpenContext ctx) {
    this.count = getRuntimeContext().getState(COUNT_DESCRIPTOR);
  }

  @Override
  public void processElement(Event event, Context ctx, Collector<Result> out)
      throws Exception {
    long next = Objects.requireNonNullElse(count.value(), 0L) + 1L;
    count.update(next);
    out.collect(new Result(event.id(), next));
  }
}
// + Kafka source, sink, watermarks, keyBy,
//   checkpoint config, restart strategy, etc.
```

…with the same logic as a StateFun function:

```java
public final class Counter implements StatefulFunction {

  private static final EgressIdentifier<Result> RESULTS =
      new EgressIdentifier<>("io.kzm.counter", "results", Result.class);

  @Persisted
  private final PersistedValue<Long> count =
      PersistedValue.of("count", Long.class);

  @Override
  public void invoke(Context context, Object input) {
    if (input instanceof Event event) {
      long next = Optional.ofNullable(count.get()).orElse(0L) + 1L;
      count.set(next);
      context.send(RESULTS, new Result(event.id(), next));
    }
  }
}
// Routing, ingress, egress, checkpoints
// declared in module.yaml.
```

That's the whole pitch: per-key durable state and exactly-once messaging without authoring the Flink topology yourself. It maps cleanly onto fraud detection, IoT digital twins, payment sagas — anything where state is keyed by a logical id and you'd rather not roll your own Flink job around it.

## The state of upstream Apache Stateful Functions

The [upstream repo][upstream] is at version **3.4.0**, released **October 2024**. That release targets Flink 1.16 and Java 11. The mailing list is quiet, the PR queue is dormant, and there's no public roadmap.

This isn't a criticism of the upstream maintainers — open-source projects come and go from active development for legitimate reasons. Committer attention is finite, vendors shift focus, the world moves on. It's a statement of fact: if you adopt upstream Stateful Functions today, you're adopting a piece of software the upstream community is no longer actively investing in.

For some teams, that's fine. The API is stable, the runtime is mature, the model is sound. For others, it's a non-starter — particularly if you need Flink 2.x for any of its new capabilities.

## What Flink 2.x changed

Flink 2.0 shipped in 2025; 2.2 followed shortly after. The headline changes:

- **Disaggregated state** — RocksDB state can live in object storage with a smarter caching layer in front, decoupling state size from local disk and unlocking horizontal scaling for stateful jobs that previously hit per-TM disk ceilings.
- **A modernized configuration system** — `state.backend.type` instead of `state.backend`, `high-availability.type` instead of `high-availability`, and many other YAML keys that follow a consistent prefix-namespacing pattern.
- **Java 17 minimum / Java 21 supported** — upstream Stateful Functions still pins Java 11.
- **A revamped Kinesis connector** — `KinesisStreamsSource` with proper Flink 2.x source/sink APIs, replacing the older `FlinkKinesisConsumer`.
- **Stricter type system** for keyed state, surfacing latent bugs in serializers that 1.x silently tolerated.

None of these are bolt-on patches. They're runtime-deep changes, and porting Stateful Functions to ride them takes more than a version bump in the POM.

## What we changed

The surface area of the work — full details in [the migration guide][migration-guide]:

### Build matrix

- Java 11 → **Java 21**
- Flink 1.16 → **Flink 2.2**
- JUnit 4 → **JUnit 5.11** (every test migrated, message-last argument order, Hamcrest pinned explicitly)
- `maven-shade-plugin 3.6.1` for `statefun-protobuf-shaded` — bytecode-level relocation, replacing the source-level `replacer-plugin` (last released 2017)
- Module structure preserved — same `statefun-flink-runner`, `statefun-sdk-java`, etc. SDK consumers don't break.

### Connector replumbing

- **Kafka** — straightforward. The Flink 2.x Kafka connector is API-compatible enough that the StateFun ingress/egress layer needed targeted updates, not rewrites.
- **Kinesis** — the heavier lift. The old `FlinkKinesisConsumer` is gone in 2.x. We rewrote the StateFun Kinesis ingress and egress on top of `KinesisStreamsSource` and `KinesisStreamsSink`. One subtle invariant: the new connector hands the **stream ARN** (not the short name) to `KinesisDeserializationSchema.deserialize`, which means the routing layer's lookup table has to be re-keyed by ARN. Easy to miss; we caught it in the K8s end-to-end suite.

### State backend keys

Upstream `module.yaml` snippets that worked on Flink 1.16 don't work on 2.x because of the configuration key changes. We didn't try to rewrite them transparently — clear migration is more honest than silent translation. The migration guide spells out what to change.

### CI and release pipeline

- A new end-to-end suite that boots a real `kind` cluster, deploys via the **Flink Kubernetes Operator 1.11**, runs both Kafka and Kinesis ingress/egress flows against LocalStack (one pod for Kinesis + S3), and tears the cluster down deterministically. This is the gate before every release.
- Maven Central publishing under `io.github.kzmlabs.flinkstatefun` — pulled the same way you'd pull any upstream Apache library.
- GHCR-published Docker images for the StateFun Flink runtime, signed via Sigstore keyless attestation, scanned with Trivy, with SLSA build provenance.

### Documentation site

- Full MkDocs Material site at [kzmlabs.github.io/flink-statefun][docs], replacing the old upstream Hugo site.
- Two worked examples — fraud detection over Kafka, IoT digital twins over Kinesis — both with full Java code and `module.yaml`.

## What stayed the same

This is the part to emphasize: **the programming model is unchanged.**

If your existing app uses upstream `StatefulFunction`, `Context`, `ValueSpec`, `Address`, `RoutableMessage` — they all work. The remote-function HTTP protocol is unchanged. The `module.yaml` schema is mostly unchanged (one or two keys updated for Flink 2.x, called out in the migration guide).

We wanted migration to feel like a Flink major-version bump, not a framework rewrite. The whole point of forking — sorry, of *continuing* — is that the model was good. The model didn't need to change. The runtime under it did.

## A note on naming

We don't call this a "fork" anymore. GitHub detached the repo from `apache/flink-statefun`'s fork network in April 2026 (we asked GitHub support — the standard process for projects that have substantively diverged). It's a **continuation**: derived from Apache Stateful Functions, no longer in the same upstream lineage, governed by us, released on our cadence.

This isn't a hostile fork. We didn't start it because we disagreed with anyone. We started it because the project we depended on stopped shipping releases for the runtime we needed to deploy on, and we needed to ship our products. Apache Stateful Functions remains an Apache Software Foundation project, with its own license (Apache 2.0), provenance, and governance. We respect that, we link to it, we credit it, and we keep our docs explicit about which features came from upstream and which we added.

## When should you use this?

### Is Apache Stateful Functions still maintained?

Short answer: the upstream Apache project hasn't shipped a release since October 2024. The continuation `kzmlabs/flink-statefun` is the actively maintained line — Apache Flink 2.2, Java 21, tagged releases on a regular cadence, and a Kubernetes end-to-end gate before anything ships to Maven Central.

### Use `kzmlabs/flink-statefun` when you need

- **Stateful Functions on Apache Flink 2.x** (Flink 2.0, 2.1, 2.2) with Java 17 or Java 21. Upstream Apache Stateful Functions still pins Flink 1.16 and Java 11.
- **Durable per-key state with exactly-once messaging** — without writing a Flink job by hand. Replaces hand-rolled `KeyedProcessFunction` + checkpoint config boilerplate with a function-and-state programming model.
- **Polyglot stateful microservices** — write functions in Java, Python, Go, or JavaScript over the same StateFun remote-function HTTP protocol, share state and routing through Flink, scale function pods independently of the runtime.
- **Kafka and Kinesis stateful streaming** — first-class ingress and egress on both, with the Flink 2.x `KinesisStreamsSource` and `KinesisStreamsSink` properly integrated. Upstream's Kinesis support stopped at the Flink 1.x `FlinkKinesisConsumer` and never made the jump.
- **Kubernetes-native deployment of stateful streaming jobs** — the Flink Kubernetes Operator deploys StateFun the same way it deploys any other Flink job. Every release is gated on a real `kind` + Operator + LocalStack run with the actual remote-function pod, S3 checkpoints, and both Kafka and Kinesis verified end-to-end.
- **A migration path off Apache Stateful Functions 3.4.0** — same programming model, one Maven coordinate change (`org.apache.flink:statefun-*` → `io.github.kzmlabs.flinkstatefun:statefun-*`), and a documented upgrade for the handful of `module.yaml` keys that moved in Flink 2.x. Most user code compiles unchanged. See the [migration guide][migration-guide].
- **Production-grade supply-chain provenance** — every release is signed via Sigstore keyless attestation, scanned with Trivy, tracked by [OpenSSF Scorecard](https://scorecard.dev/viewer/?uri=github.com/kzmlabs/flink-statefun), and container images carry SLSA build provenance for `gh attestation verify`.

### Don't use it if

- You're already running upstream Apache Stateful Functions successfully on Flink 1.16 with no CVE pressure, no Java 11 ceiling, and no dependency conflicts pulling you toward a newer stack. There's no urgency to migrate purely because the version number is higher. Stability is a feature.
- You don't actually need stateful actors. If your processing is stateless or windowed-aggregation-only, vanilla Apache Flink, Kafka Streams, or ksqlDB may fit better — the StateFun model adds value specifically when you need durable per-key state with addressable, message-passing functions on top of a stream-processing runtime.

### How does this compare to Apache Flink, Kafka Streams, ksqlDB, or Akka?

- **vs. vanilla Apache Flink** — same runtime underneath, but you write functions keyed by a logical id instead of authoring DataStream / Table API jobs. Trade some flexibility for much faster development of stateful, event-driven systems.
- **vs. Kafka Streams** — Kafka Streams is excellent for Kafka-only stateful pipelines bounded to the JVM. StateFun runs on Flink, supports Kinesis, and gives you polyglot remote functions over HTTP.
- **vs. ksqlDB** — ksqlDB is SQL-first and shines for declarative stream transformations. StateFun is for actor-style code with per-key state and command/control patterns.
- **vs. Akka / Pekko cluster sharding** — Akka gives you actors but you own the persistence, supervision, and rebalancing. StateFun gives you actors backed by Flink's checkpointing, exactly-once delivery, and operator-managed scale-out.

## Where to find it

- **Source:** [github.com/kzmlabs/flink-statefun][our-repo]
- **Docs:** [kzmlabs.github.io/flink-statefun][docs]
- **Maven Central:** `io.github.kzmlabs.flinkstatefun`
- **Docker images:** `ghcr.io/kzmlabs/flink-statefun`
- **Awesome list:** [github.com/kzmlabs/awesome-statefun][awesome] — curated resources for the broader stateful-actor ecosystem
- **Migration guide:** [from upstream][migration-guide]

If you're trying it out and something breaks, file an issue. If you're using it in production and it's working, drop a star — the only way a continuation like this stays viable is if the people using it tell us they're using it.

[upstream]: https://github.com/apache/flink-statefun
[our-repo]: https://github.com/kzmlabs/flink-statefun
[docs]: https://kzmlabs.github.io/flink-statefun/
[migration-guide]: ../upstream-vs-kzm.md
[awesome]: https://github.com/kzmlabs/awesome-statefun
