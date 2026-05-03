# Awesome-list submissions index

Ready-to-paste submission bundles for the curated lists where Kzmlabs StateFun Actors (and `kzmlabs/awesome-statefun`) belong. Each file has the bullet, the recommended section, and a per-audience PR body.

| File | Target list | What gets submitted |
|---|---|---|
| [`awesome-flink.md`](awesome-flink.md) | wuchong/awesome-flink | `kzmlabs/flink-statefun` |
| [`sindresorhus-awesome.md`](sindresorhus-awesome.md) | sindresorhus/awesome | `kzmlabs/awesome-statefun` |
| [`awesome-streaming.md`](awesome-streaming.md) | manuzhang/awesome-streaming | `kzmlabs/flink-statefun` |
| [`awesome-kafka.md`](awesome-kafka.md) | infoslack/awesome-kafka | `kzmlabs/flink-statefun` (Kafka I/O angle) |
| [`awesome-kubernetes.md`](awesome-kubernetes.md) | ramitsurana/awesome-kubernetes | `kzmlabs/flink-statefun` (K8s deployment angle) |

## Suggested order

1. **awesome-flink** first — most relevant audience, fastest review cycle, validates the bullet copy.
2. **awesome-streaming** + **awesome-kafka** + **awesome-kubernetes** in parallel — independent reviews, different audiences.
3. **sindresorhus/awesome last** — strict lint, long review queue (4–12 weeks). Polish `awesome-statefun` first; this sub will get scrutinized hardest.

## Common pre-flight

Before opening any PR:

- [ ] All links in the submission body resolve to https (no http)
- [ ] Repo description on `kzmlabs/flink-statefun` matches the bullet description
- [ ] Repo topics include the keywords the bullet uses (apache-flink, statefun, kafka, kinesis, kubernetes, stateful-functions)
- [ ] Latest release tag is visible on the repo home page (rich previews show release date)

## What we're NOT submitting (and why)

- **Generic "awesome-stream-processing" lists** without active maintainers — dead lists waste reviewer goodwill on the live ones.
- **Lists in languages we can't review back** (Chinese-only forks of awesome-flink) — fine to be linked from there organically, but don't open PRs you can't follow up on.
- **Lists where we'd be the only entry under a section** — that's a sign the list isn't what we think it is.
