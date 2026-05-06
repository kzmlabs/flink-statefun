> ## 🚧 Under development — not released
>
> This SDK is **inherited as-is from [Apache Stateful Functions](https://github.com/apache/flink-statefun)** and is **not actively maintained on the Flink 2.x runtime in this fork**.
>
> - **Not in the Maven reactor** — `mvn -B verify` does not build this module.
> - **Not in CI** — no automated build, test, or version bump runs against this code.
> - **Not released** — no Go module publication from this fork; not part of any release tag.
> - **May be revived** if a consumer drives the modernization (toolchain, deps, protocol, CI). See [issue #160](https://github.com/kzmlabs/flink-statefun/issues/160) for the re-introduction policy.
>
> **For non-Java users today**: integrate via the [request/reply HTTP protocol](https://nightlies.apache.org/flink/flink-statefun-docs-release-3.4/docs/sdk/external/) directly. The wire format is language-agnostic; an official SDK is convenience, not a hard requirement.

---

# StateFun Go SDK

Go SDK for [Apache Stateful Functions](https://github.com/apache/flink-statefun), inherited from upstream and currently kept frozen in this fork. See the status banner above.

The source is preserved in-tree as a starting point for any future contributor who wants to take ownership of reviving the module — including modernization to current StateFun protocol revisions, current Go toolchain, dependency hygiene, and CI integration. Until then, treat this directory as **archival reference**, not as production code.

## Why we keep it

Deleting unmaintained code makes a "Java-only fork" statement that is stronger than what we actually mean. The fork's runtime is language-agnostic at the wire level — `request/reply` over HTTP+protobuf — and any language can call it. The only thing missing is an officially-maintained convenience wrapper. Keeping the upstream code in-tree (as a frozen reference) leaves the door open without making promises we can't keep on Flink 2.x compatibility today.

See [issue #160](https://github.com/kzmlabs/flink-statefun/issues/160) for the policy on re-introducing this as a first-class module.
