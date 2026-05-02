# Install

## Maven coordinates

The Kzmlabs StateFun artifacts are published to Maven Central under the `io.github.kzmlabs.flinkstatefun` group.

=== "BOM (recommended)"

    Import the BOM in your `<dependencyManagement>` to keep all StateFun module versions aligned.

    ```xml
    <dependencyManagement>
      <dependencies>
        <dependency>
          <groupId>io.github.kzmlabs.flinkstatefun</groupId>
          <artifactId>statefun-bom</artifactId>
          <version>3.4.0-KZM-3.1</version>
          <type>pom</type>
          <scope>import</scope>
        </dependency>
      </dependencies>
    </dependencyManagement>
    ```

    Then declare modules without versions:

    ```xml
    <dependency>
      <groupId>io.github.kzmlabs.flinkstatefun</groupId>
      <artifactId>statefun-sdk-java</artifactId>
    </dependency>
    ```

=== "Direct"

    ```xml
    <dependency>
      <groupId>io.github.kzmlabs.flinkstatefun</groupId>
      <artifactId>statefun-sdk-java</artifactId>
      <version>3.4.0-KZM-3.1</version>
    </dependency>
    ```

## Docker image

The StateFun runtime is published to GitHub Container Registry:

```bash
docker pull ghcr.io/kzmlabs/flink-statefun:3.4.0-KZM-3.1
```

The image bundles Flink 2.2 + Java 21 + the StateFun distribution JARs + S3/OSS/Azure plugin filesystems, ready for deployment via the Flink Kubernetes Operator.

## Module overview

| Module | Description |
|---|---|
| `statefun-sdk-java` | Java SDK for remote functions |
| `statefun-sdk-embedded` | Embedded SDK for co-located functions |
| `statefun-flink-core` | Core Flink integration |
| `statefun-flink-distribution` | Distribution JAR for deployment |
| `statefun-kafka-io` | Kafka ingress/egress connectors |
| `statefun-kinesis-io` | AWS Kinesis ingress/egress connectors |
| `statefun-flink-runner` | Uber JAR for K8s deployment via Flink Operator |
| `statefun-bom` | Bill of Materials for dependency version alignment |

## Version matrix

| Kzmlabs version | Apache StateFun base | Flink | Java |
|---|---|---|---|
| `3.4.0-KZM-3.1` | 3.4.0 | 2.2.0 | 21 |
| `3.4.0-KZM-3.0` | 3.4.0 | 2.2.0 | 21 |
| `3.4.0-KZM-2.0` | 3.4.0 | 2.2.0 | 21 |

Upstream Apache StateFun 3.4.0 targets Flink 1.16 + Java 11. The Kzmlabs `KZM-x.y` line tracks Flink 2.x and Java 21.
