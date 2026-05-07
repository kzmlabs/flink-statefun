# Changelog

All notable changes to **StateFun Actors by Kzmlabs** are documented in this file. The fork continues the [Apache Stateful Functions](https://github.com/apache/flink-statefun) programming model on Apache Flink 2.x and Java 21.

> **Reading guide:** "KZM-x.y" releases are this fork's versions on Flink 2.x. Apache StateFun's last release was [3.4.0](https://github.com/apache/flink-statefun/releases/tag/release-3.4.0) (October 2024) on Flink 1.16. See [docs/upstream-vs-kzm.md](https://kzmlabs.github.io/flink-statefun/upstream-vs-kzm/) for the full migration matrix.

## [3.4.0-KZM-3.1] - 2026-04-29

Patch release on top of 3.4.0-KZM-3.0. Same Kinesis-restoration baseline as
3.0; this entry exists for traceability so the changelog and Maven Central
match.

## [3.4.0-KZM-3.0] - 2026-04-29

Stable promotion of `3.4.0-KZM-3.0-RC1` after the Kubernetes native E2E gate
(both Kafka and Kinesis suites) ran green and Maven Central + GHCR publishing
completed cleanly. No source changes vs RC1 — same Kinesis I/O restoration,
same LocalStack-backed K8s E2E.

## [3.4.0-KZM-3.0-RC1] - 2026-04-24

### Added — Kinesis ingress/egress end-to-end support

Restores full Kinesis I/O for Flink 2.x by wiring the `statefun-kinesis-io`
SDK specs to Flink 2.x's `KinesisStreamsSource` / `KinesisStreamsSink`
(connector `org.apache.flink:flink-connector-aws-kinesis-streams:6.0.0-2.0`,
Source V2 / Sink V2 APIs). Closes the 3.4.0-KZM-2.0 regression where the
SDK jar published to Maven Central without a runtime binding.

**Runtime wiring (`statefun-flink-io-bundle`)**
- `KinesisFlinkIoModule` registers source/sink providers via
  `@AutoService(FlinkIoModule.class)`.
- `KinesisSourceProvider` builds `KinesisStreamsSource` from
  `KinesisIngressSpec`; maps the SDK's `KinesisIngressStartupPosition`
  (LATEST/EARLIEST/AT_DATE) to `KinesisSourceConfigOptions.InitialPosition`.
- `KinesisSinkProvider` builds `KinesisStreamsSink` from `KinesisEgressSpec`.
- Shared `AwsConfigAppender` maps SDK `AwsCredentials` / `AwsRegion` into
  `AWSConfigConstants` entries.
- JSON v1 binders (`io.statefun.kinesis.v1/ingress` and `/egress`) restored
  for `module.yaml` parsing.
- 34 unit tests.

**SDK extensions (`statefun-kinesis-io`)**
- `KinesisIngressSpec.streamArn()` + `KinesisIngressBuilder.withStreamArn(arn)`
  — Flink 2.x Kinesis source requires a stream ARN. Backwards compatible
  with legacy `withStream(name)`.
- `KinesisEgressSpec.streamName()` + `KinesisEgressBuilder.withStreamName(name)`
  — Flink 2.x Kinesis sink is pre-bound to a single stream; `EgressRecord.getStream()`
  is documented as runtime-ignored.
- `AwsRegion.CustomEndpointAwsRegion` now accepts `http://` in addition to
  `https://` (required for LocalStack in K8s E2E; real AWS usage unaffected).

**Kubernetes native E2E gate**
- LocalStack 4.1 deployed alongside Kafka + MinIO in the existing
  `statefun-k8s-native-e2e` kind cluster.
- `counter-commands` / `counter-results` Kinesis streams pre-created via
  `awslocal`.
- New `KinesisCounterFn` function mirrors `KafkaCounterFn` (renamed from
  `CounterFn` for symmetry) — same `CounterCommand` / `CounterResult`
  proto contract, different transport.
- `StateFunKinesisE2E` tagged `@Tag("kinesis")`, runs alongside
  `@Tag("kafka")` `StateFunK8sE2E`. Granular selection via
  `-Dgroups=kinesis` or skip via `-DexcludedGroups=kinesis`.
- Shared `KubectlPortForward` helper (AutoCloseable) replaces duplicated
  `ProcessBuilder` boilerplate.
- Both suites are release-blocking in `release.yml` / `docker-release.yml`.

## [3.4.0-KZM-2.0] - 2026-04-23

First stable release of the KZM-2.0 line, promoting RC7 after successful Maven Central and GHCR validation, Kubernetes native E2E testing, and dependency/plugin modernization completed across RC1–RC7.

### Highlights (cumulative since 3.4.0-KZM-1.0)

- **Flink 2.2.0 + Java 21** baseline with `io.github.kzmlabs.flinkstatefun` groupId
- **Kubernetes native E2E test suite** with Flink K8s Operator 1.11 (Kafka, MinIO, RocksDB checkpointing) — mandatory release gate
- **JUnit Jupiter 5.11.4** migration across all modules
- **Maven Central** publishing with Sonatype central-publishing-maven-plugin 0.10.0
- **GHCR Docker image** (`ghcr.io/kzmlabs/flink-statefun`) — `latest` tag now published for stable releases
- **Dependency modernization**: Kafka clients 3.9.1, OkHttp 4.12.0, Testcontainers 2.0.3, Hamcrest 3.0, Protobuf 3.25.5, commons-lang3 3.20.0, commons-compress 1.28.0
- **Plugin modernization**: maven-compiler 3.13.0, maven-shade 3.6.1, surefire/failsafe 3.5.5, enforcer 3.6.2, rat 0.17, javadoc 3.12.0, gpg 3.2.8
- **statefun-bom** module for centralized dependency management
- **statefun-flink-runner** uber-JAR module with K8s-ready Docker image

## [3.4.0-KZM-2.0-RC7] - 2025-02-25

### Testing
- Migrate from JUnit 4 to JUnit Jupiter 5.11.4
- Convert all test annotations to JUnit 5 equivalents
- Convert parameterized tests to @ParameterizedTest/@MethodSource
- Add explicit hamcrest 3.0 dependency (no longer transitive from JUnit 5)

### Dependencies
- Upgrade Testcontainers 1.20.4 → 2.0.3
- Upgrade Hamcrest 2.2 → 3.0
- Upgrade junixsocket 2.3.2 → 2.10.1
- Upgrade LZ4 1.8.0 → 1.8.1
- Upgrade commons-lang3 3.18.0 → 3.20.0
- Upgrade commons-compress 1.26.0 → 1.28.0 (pin commons-io to 2.15.1)

### Maven Plugins
- Upgrade maven-surefire/failsafe 3.5.2 → 3.5.5
- Upgrade maven-shade-plugin 3.6.0 → 3.6.1
- Upgrade maven-enforcer-plugin 3.5.0 → 3.6.2
- Upgrade apache-rat-plugin 0.13 → 0.17
- Upgrade maven-source-plugin 3.3.0 → 3.4.0
- Upgrade maven-javadoc-plugin 3.6.3 → 3.12.0
- Upgrade maven-gpg-plugin 3.1.0 → 3.2.8
- Upgrade central-publishing-maven-plugin 0.6.0 → 0.10.0
- Upgrade exec-maven-plugin 3.5.0 → 3.6.3

## [3.4.0-KZM-2.0-RC6] - 2025-02-25

### Dependencies
- Upgrade JUnit 4.12 → 4.13.2
- Upgrade Hamcrest 1.3 → 2.2 (artifact rename: hamcrest-all → hamcrest)
- Upgrade Auto-Service 1.0-rc6 → 1.1.1
- Upgrade OkHttp 3.14.6 → 4.12.0
- Upgrade Kafka clients 2.4.1 → 3.9.1 (aligned with Flink connector)
- Upgrade Testcontainers 1.15.2 → 1.20.4
- Upgrade JMH 1.21 → 1.37
- Upgrade JimFS 1.1 → 1.3.0
- Fix SLF4J version inconsistency (1.7.7 → 1.7.36 via parent property)

### Maven Plugins
- Unify maven-shade-plugin to 3.6.0 across all modules
- Upgrade maven-surefire/failsafe 2.22.x → 3.5.2
- Upgrade maven-enforcer-plugin 3.0.0-M2 → 3.5.0
- Upgrade exec-maven-plugin 1.6.0 → 3.5.0
- Fix protoc-jar-maven-plugin version inconsistency (3.11.1 → 3.11.4)

### CI/CD
- Upgrade actions/checkout v2 → v4 in doc-check workflow
- Add 20-minute timeout to CI build
- Add concurrency limits to all workflows

### Cleanup
- Remove unused Helm chart (tools/k8s/)
- Remove stale Flink 1.12 TODO in InputStreamUtils
- Remove misleading TODO in StatefulFunctionsUniverseValidator
- Update docs/config.toml for Kzmlabs fork
- Add CODEOWNERS file
- Update copyright year to 2025

## [3.4.0-KZM-2.0-RC5] - 2025-02-25

- Add CHANGELOG.md with release history
- Only push Docker `latest` tag for stable (non-RC) releases
- Remove empty non-Java SDK modules from Maven reactor
- Fix FlinkDeployment example: `flinkVersion` v2_2 → v2_0 (Operator 1.11 limit)
- Remove `UnixDomainSocketITCase` exclusion from surefire (self-skips on Windows)
- Re-enable `EmbeddedSmokeHarnessTest` with reduced params and Flink 2.2 config fixes
- Upgrade `maven-compiler-plugin` 3.8.1 → 3.13.0
- Upgrade `protobuf` 3.7.1 → 3.25.5 and `protoc-jar-maven-plugin` 3.11.1 → 3.11.4
- Add `statefun-bom` module for dependency management

## [3.4.0-KZM-2.0-RC4] - 2025-02-25

- Fix CI: remove `-Dtest` override, exclude `UnixDomainSocketITCase` in pom.xml
- Cleanup stale files

## [3.4.0-KZM-2.0-RC3] - 2025-02-24

- CI: run all unit/integration tests, skip E2E modules
- Move all tests to release pipeline, CI compile-only

## [3.4.0-KZM-2.0-RC2] - 2025-02-24

- Add K8s native E2E tests with Flink Kubernetes Operator
- Fix protobuf-shaded build (maven-shade-plugin 3.6.0)
- Add statefun-flink-runner uber JAR module with K8s-ready Docker image
- Change groupId to `io.github.kzmlabs.flinkstatefun`
- Add release documentation and Docker-only workflow
- Add `<name>` element to all modules for Maven Central validation
- Iterative Maven Central publishing fixes (RC1 through RC18)

## [3.4.0-KZM-2.0-RC1] - 2025-02-14

- Fork Apache Flink StateFun for Maven Central publishing under `io.github.kzmlabs.flinkstatefun`
- Upgrade to Flink 2.2.0 and Java 21
- Add Docker image publishing to GitHub Container Registry
- Add release setup guide and release script

[3.4.0-KZM-3.1]: https://github.com/kzmlabs/flink-statefun/releases/tag/v3.4.0-KZM-3.1
[3.4.0-KZM-3.0]: https://github.com/kzmlabs/flink-statefun/releases/tag/v3.4.0-KZM-3.0
[3.4.0-KZM-3.0-RC1]: https://github.com/kzmlabs/flink-statefun/releases/tag/v3.4.0-KZM-3.0-RC1
[3.4.0-KZM-2.0]: https://github.com/kzmlabs/flink-statefun/releases/tag/v3.4.0-KZM-2.0
[3.4.0-KZM-2.0-RC7]: https://github.com/kzmlabs/flink-statefun/releases/tag/v3.4.0-KZM-2.0-RC7
[3.4.0-KZM-2.0-RC6]: https://github.com/kzmlabs/flink-statefun/releases/tag/v3.4.0-KZM-2.0-RC6
[3.4.0-KZM-2.0-RC5]: https://github.com/kzmlabs/flink-statefun/releases/tag/v3.4.0-KZM-2.0-RC5
[3.4.0-KZM-2.0-RC4]: https://github.com/kzmlabs/flink-statefun/releases/tag/v3.4.0-KZM-2.0-RC4
[3.4.0-KZM-2.0-RC3]: https://github.com/kzmlabs/flink-statefun/releases/tag/v3.4.0-KZM-2.0-RC3
[3.4.0-KZM-2.0-RC2]: https://github.com/kzmlabs/flink-statefun/releases/tag/v3.4.0-KZM-2.0-RC2
[3.4.0-KZM-2.0-RC1]: https://github.com/kzmlabs/flink-statefun/releases/tag/v3.4.0-KZM-2.0-RC1
