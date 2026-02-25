# Changelog

All notable changes to the Kzmlabs StateFun fork are documented in this file.

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

[3.4.0-KZM-2.0-RC5]: https://github.com/kzmlabs/flink-statefun/releases/tag/v3.4.0-KZM-2.0-RC5
[3.4.0-KZM-2.0-RC4]: https://github.com/kzmlabs/flink-statefun/releases/tag/v3.4.0-KZM-2.0-RC4
[3.4.0-KZM-2.0-RC3]: https://github.com/kzmlabs/flink-statefun/releases/tag/v3.4.0-KZM-2.0-RC3
[3.4.0-KZM-2.0-RC2]: https://github.com/kzmlabs/flink-statefun/releases/tag/v3.4.0-KZM-2.0-RC2
[3.4.0-KZM-2.0-RC1]: https://github.com/kzmlabs/flink-statefun/releases/tag/v3.4.0-KZM-2.0-RC1
