---
title: Install
description: Maven Central coordinates, Bill of Materials import, Docker image, and supported version matrix for StateFun Actors — Apache Flink 2.2 + Java 21 stateful actor framework.
---

# Install

> Add StateFun Actors to your project — Maven Central artifacts, Docker image, signed releases.

## Maven artifacts

The full module set is published to Maven Central under the `io.github.kzmlabs.flinkstatefun` group.

=== "BOM (recommended)"

    Import the BOM in your `<dependencyManagement>` once, then reference modules without versions everywhere else. The BOM keeps the StateFun module set, Flink, Java, Protobuf, and AWS SDK all aligned to a tested combination.

    ```xml
    <dependencyManagement>
      <dependencies>
        <dependency>
          <groupId>io.github.kzmlabs.flinkstatefun</groupId>
          <artifactId>statefun-bom</artifactId>
          <version>3.4.0-KZM-3.3</version>
          <type>pom</type>
          <scope>import</scope>
        </dependency>
      </dependencies>
    </dependencyManagement>

    <dependencies>
      <dependency>
        <groupId>io.github.kzmlabs.flinkstatefun</groupId>
        <artifactId>statefun-sdk-java</artifactId>
      </dependency>
    </dependencies>
    ```

=== "Direct"

    Pin each module's version explicitly. Works, but you'll be on the hook for keeping versions consistent.

    ```xml
    <dependency>
      <groupId>io.github.kzmlabs.flinkstatefun</groupId>
      <artifactId>statefun-sdk-java</artifactId>
      <version>3.4.0-KZM-3.3</version>
    </dependency>
    ```

=== "Gradle (Kotlin)"

    ```kotlin
    dependencies {
      implementation(platform("io.github.kzmlabs.flinkstatefun:statefun-bom:3.4.0-KZM-3.3"))
      implementation("io.github.kzmlabs.flinkstatefun:statefun-sdk-java")
    }
    ```

## Docker image

Container images are published to GitHub Container Registry:

```bash
docker pull ghcr.io/kzmlabs/flink-statefun:3.4.0-KZM-3.3
```

The image bundles **Flink 2.2 + Java 21 + the StateFun distribution JARs + S3/OSS/Azure plugin filesystems**, ready to run as a `FlinkDeployment` via the Flink Kubernetes Operator.

!!! tip "Verify the image signature"

    Releases are signed with [Sigstore keyless attestation](https://docs.sigstore.dev/) and carry [SLSA build provenance](https://slsa.dev/). Verify with:

    ```bash
    gh attestation verify oci://ghcr.io/kzmlabs/flink-statefun:3.4.0-KZM-3.3 --owner kzmlabs
    ```

## Module overview

| Module | Purpose |
|---|---|
| `statefun-sdk-java` | Java SDK for remote functions (HTTP-served) |
| `statefun-sdk-embedded` | Embedded SDK for co-located functions |
| `statefun-flink-core` | Core Flink integration |
| `statefun-flink-distribution` | Distribution JAR for deployment |
| `statefun-flink-runner` | Uber JAR — drop-in for the Flink Kubernetes Operator |
| `statefun-kafka-io` | Kafka ingress / egress connectors |
| `statefun-kinesis-io` | AWS Kinesis ingress / egress connectors |
| `statefun-shaded` | Relocated Protobuf to avoid version conflicts (transitive) |
| `statefun-bom` | Bill of Materials for version alignment |

## Version matrix

| Kzmlabs version | Apache StateFun base | Flink | Java | Status |
|---|---|---|---|---|
| **`3.4.0-KZM-3.3`** | 3.4.0 | 2.2.0 | 21 | **Latest stable** |
| `3.4.0-KZM-3.1` | 3.4.0 | 2.2.0 | 21 | Stable |
| `3.4.0-KZM-3.0` | 3.4.0 | 2.2.0 | 21 | Stable |

Apache StateFun 3.4.0 itself targets Flink 1.16 and Java 11. The `KZM-x.y` line tracks Flink 2.x and Java 21.

!!! warning "Java baseline"

    All `KZM-x.y` releases require **Java 21** at runtime. Building against Java 17 or earlier will not work — see the [build guide](build.md) for the full toolchain.

## Snapshots

`-SNAPSHOT` builds are published to Maven Central's snapshot repository:

```xml
<repositories>
  <repository>
    <id>central-snapshots</id>
    <url>https://central.sonatype.com/repository/maven-snapshots/</url>
    <snapshots><enabled>true</enabled></snapshots>
  </repository>
</repositories>
```

Use snapshots for testing fixes before they're released; pin a stable version for production.

## Next steps

<div class="grid cards" markdown>

- :material-rocket-launch:{ .lg .middle } &nbsp; **[Quickstart](quickstart.md)** — first round-trip message in five minutes.
- :material-source-branch:{ .lg .middle } &nbsp; **[Build from source](build.md)** — full reactor build, restricted-network mirrors, formatter.
- :material-server-network:{ .lg .middle } &nbsp; **[Kafka I/O guide](guides/kafka-io.md)** — `module.yaml` ingress and egress configuration.
- :material-source-branch-sync:{ .lg .middle } &nbsp; **[Migrate from Apache StateFun](upstream-vs-kzm.md)** — what changes, what stays the same.

</div>
