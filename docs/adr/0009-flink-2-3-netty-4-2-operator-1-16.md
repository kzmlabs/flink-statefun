---
title: "ADR-0009: Flink 2.3.0, shaded Netty 4.2 and Operator 1.16.1"
description: Moving the StateFun runtime to Apache Flink 2.3.0, which changes the shaded Netty line the remote-function HTTP client is built on, and aligning the Kubernetes Operator and FlinkDeployment CRs with it.
---

# ADR-0009: Flink 2.3.0, shaded Netty 4.2 and Operator 1.16.1

| | |
|---|---|
| Status | Accepted |
| Date | 2026-11-02 |
| Issues/PRs | this PR |

## Context

Apache Flink 2.3.0 (released 2026-06-25) is the current stable line. Its user-facing changes are concentrated in Table/SQL, the native S3 filesystem, the adaptive scheduler and application management, none of which StateFun uses. The relevant changes for this runtime are in the dependency layer, verified against the published `flink-parent` 2.3.0 POM:

| Dependency | Flink 2.2.1 | Flink 2.3.0 | Ticket |
|---|---|---|---|
| flink-shaded | 20.0 | 21.0 | FLINK-38903 |
| flink-shaded-netty | 4.1.100.Final-20.0 | **4.2.6.Final-21.0** | FLINK-38912 |
| flink-shaded-jackson | 2.18.2-20.0 | 2.20.1-21.0 | FLINK-38855 |
| log4j (image `lib/`) | 2.24.3 | 2.25.3 | FLINK-39146 |
| lz4 fork (`at.yawk.lz4`) | 1.10.3 | 1.10.3 | unchanged |
| slf4j-api | 1.7.36 | 1.7.36 | unchanged |

StateFun's remote-function transport (`statefun-flink-core/.../nettyclient`) is written directly against the shaded Netty classes that `flink-dist` ships in `lib/`. The module compiled against `flink-shaded-netty` 4.1.91.Final-17.0, three shaded releases behind the runtime it was deployed on, and the uber `statefun-flink-distribution.jar` bundled those 4.1.91 classes next to `flink-dist.jar` in `/opt/flink/lib`. With Flink 2.3 the runtime copy becomes Netty 4.2, a line that Netty documents as not coexisting with 4.1 on one classpath.

Netty 4.2 keeps the public API StateFun uses (`Bootstrap`, `FixedChannelPool`, `ChannelHealthChecker`, `HttpClientCodec`, `SslContextBuilder`) but changes three things that matter here:

1. Transport-specific event loop groups (`NioEventLoopGroup`, `EpollEventLoopGroup`, `KQueueEventLoopGroup`) are deprecated in favour of `MultiThreadIoEventLoopGroup` plus a transport `IoHandlerFactory`.
2. `SslContextBuilder.forClient()` now defaults the endpoint identification algorithm to `HTTPS`, so TLS clients verify the server hostname against the certificate. Netty 4.1 defaulted to no verification.
3. The default `ByteBufAllocator` becomes the adaptive allocator instead of the pooled one.

On the Kubernetes side, the Flink Kubernetes Operator 1.16 line is the first to accept `flinkVersion: v2_3`. Its 1.16.1 patch fixes FLINK-40467, where a Flink 2.3 application-mode deployment managed by the operator could come back from a savepoint redeploy without submitting the job.

## Decision

- Bump `flink.version` to 2.3.0 and pin the shaded dependencies StateFun compiles against to the exact versions Flink 2.3.0 ships: `flink-shaded-netty` 4.2.6.Final-21.0 and `flink-shaded-jackson` 2.20.1-21.0. The distribution uber jar now carries the same shaded classes as `flink-dist`, removing the long-standing 4.1.91 vs runtime skew.
- Migrate the two event-loop construction sites (`NettySharedResources`, the smoke-test `CommandInterpreterAppServer`) to `MultiThreadIoEventLoopGroup` with `EpollIoHandler`/`KQueueIoHandler`/`NioIoHandler` factories. Transport selection and thread naming are unchanged. The smoke-test fixture module carried its own `flink-shaded-netty` pin at 4.1.65.Final-14.0; it is aligned to 4.2.6.Final-21.0 as well.
- Set `endpointIdentificationAlgorithm(null)` explicitly on the remote-function `SslContextBuilder`. This preserves the behaviour every existing deployment runs with: trust is established through `trustedCaCerts`, hostnames are not verified. Enabling hostname verification is a security improvement worth having, but it is a behavioural change for users whose remote-function certificates do not carry the service DNS name in their SAN, so it is deferred to a follow-up that exposes it as a per-endpoint `transport` setting rather than being switched on silently by a runtime upgrade.
- Accept the adaptive allocator default. StateFun only allocates through `ctx.channel().alloc()` for request bodies; Flink's own network stack keeps its explicit pooled allocator. No configuration is added.
- Re-pin the runtime image to `apache/flink:2.3.0-java21` by digest. The base is still Ubuntu 24.04 (noble) with Temurin 21, so the scoped `apt-get --only-upgrade` list and the `lib/` clean-up globs (`flink-table*`, `log4j-*`, which now also match the new `log4j-layout-template-json`) stay valid.
- Move the K8s E2E gate to Operator 1.16.1 and every `FlinkDeployment` CR and docs example from `flinkVersion: v2_2` to `v2_3`. The deprecated `jobManager.resource`/`taskManager.resource` fields are still accepted by 1.16 and are left as-is; moving to the new `resources` (Kubernetes `ResourceRequirements`) field is a separate change.
- Leave the connectors where they are: `flink-connector-kafka` 4.0.1-2.0 and `flink-connector-aws` 6.0.0-2.0. Both are built against the stable Flink 2.x Source/Sink v2 API and no 2.3-specific connector release exists. Kafka connector 5.0.0 moves to kafka-clients 4.2 and is a separate decision.

## Consequences

- Savepoint compatibility: Flink 2.2 to 2.3 is a minor upgrade within the 2.x line; StateFun's own state serializers are unchanged.
- Trivy baseline shifts: netty 4.2.6, jackson 2.20.1 and log4j 2.25.3 replace older copies in the image.
- Remote functions see no protocol change; the HTTP client behaves as before, including TLS trust semantics.
- The deprecated `resource` CR fields and the TLS hostname-verification knob are tracked as follow-ups.
- Validated with the full unit-test build locally; the K8s E2E gate (Kafka and Kinesis suites) runs in CI on this PR.
