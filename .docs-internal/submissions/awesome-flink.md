# Submission to awesome-flink

Ready-to-paste markdown for [wuchong/awesome-flink](https://github.com/wuchong/awesome-flink) and similar curated lists.

## Recommended placement

Section: **Libraries** or **Stateful Functions** (whichever the list uses).

## Single-line entry

```markdown
- [Kzmlabs StateFun Actors](https://github.com/kzmlabs/flink-statefun) — Stateful actors on Apache Flink 2.x and Java 21. Durable per-key state, exactly-once messaging, Kafka and Kinesis I/O, Kubernetes-native deployment.
```

## Why this phrasing

- **Leads with what it does** ("stateful actors"), not what it's a continuation of — readers self-qualify based on capability, not pedigree.
- **Names two constraints** (Flink 2.x, Java 21) — signals modernity without using marketing words like "modern", "active", or "maintained" that awesome-list reviewers typically flag as filler.
- **All SEO keywords appear naturally** — Apache Flink, stateful actors, Kafka, Kinesis, Kubernetes — without keyword-stuffing.
- **Single sentence** — fits the typical awesome-list one-line format.

## How to submit

1. Fork [wuchong/awesome-flink](https://github.com/wuchong/awesome-flink)
2. Find the appropriate section (Libraries / Stateful Functions / similar)
3. Insert the bullet alphabetically
4. Run `npx awesome-lint` if the project uses it
5. Open a PR with title: `Add Kzmlabs StateFun Actors`
6. PR body — three lines is enough:

```markdown
Adds Kzmlabs StateFun Actors, a continuation of Apache Stateful Functions
on Flink 2.x and Java 21. Maintained on Maven Central + GHCR with a
Kubernetes-native E2E gate. Fits under the Stateful Functions section.
```

## Other lists with their own submission file

Per-audience framing — see the dedicated submission file for the list-specific bullet, PR body, and pitch angle:

- [`sindresorhus-awesome.md`](sindresorhus-awesome.md) — master list of awesome lists (high authority, strict lint)
- [`awesome-streaming.md`](awesome-streaming.md) — manuzhang/awesome-streaming
- [`awesome-kafka.md`](awesome-kafka.md) — infoslack/awesome-kafka (Kafka audience)
- [`awesome-kubernetes.md`](awesome-kubernetes.md) — ramitsurana/awesome-kubernetes (K8s audience)

The headline bullet stays the same; only the PR body framing changes per audience.
