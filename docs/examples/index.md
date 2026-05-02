---
title: Examples
description: Production-shaped StateFun Actors examples on Apache Flink — financial fraud detection and IoT fleet telemetry, with full code, module.yaml wiring, and deployment notes.
---

# Examples

> Two end-to-end walkthroughs showing how the actor model, durable state, and ingress/egress fit together for non-trivial systems.

Pick whichever is closer to what you're building:

<div class="grid cards" markdown>

-   :material-credit-card-search:{ .lg .middle } &nbsp; **[Real-time fraud detection](fraud-detection.md)**

    ---

    Per-card risk scoring on a payments stream. Velocity checks, geo-impossibility detection, delayed re-evaluation. Shows actors that decay state over time and emit alerts only on threshold breach.

-   :material-chip:{ .lg .middle } &nbsp; **[IoT fleet digital twins](iot-fleet.md)**

    ---

    Per-device twins for industrial telemetry. Rolling sensor stats, battery degradation alerts, command/response loop. Shows long-lived actors, command emission back to devices, and offline-detection via timers.

</div>

## What both examples have in common

- **One actor per logical id** — one card, one device. Per-instance state, isolated by Flink's keyed-state.
- **Durable state survives restart** — RocksDB + S3 checkpoints; if the cluster failovers, state is recovered.
- **Exactly-once messaging** — Kafka transactions + Flink checkpointing; no duplicate alerts on replay.
- **Polyglot remote functions** — the function logic is Java in these examples but could be Python, Go, or JS over the same HTTP wire protocol.

## What you'll see in each example

1. **Problem framing** — the concrete business signal each system has to detect
2. **Architecture diagram** — Mermaid flow of the pipeline
3. **Protobuf message types** — wire format
4. **`StatefulFunction` implementation** — Java with annotations explaining each step
5. **`module.yaml` wiring** — ingress, egress, function endpoint
6. **Local testing** — how to send a synthetic event and verify the response
7. **Production scaling notes** — partitioning, hot-key handling, throughput targets

## Next steps

<div class="grid cards" markdown>

- :material-rocket-launch:{ .lg .middle } &nbsp; **[Quickstart](../quickstart.md)** — round-trip a message in five minutes before diving in.
- :material-graph:{ .lg .middle } &nbsp; **[Architecture overview](../architecture/index.md)** — the runtime model that powers both examples.
- :material-server-network:{ .lg .middle } &nbsp; **[Kafka I/O guide](../guides/kafka-io.md)** — full ingress/egress reference.

</div>
