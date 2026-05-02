# Kinesis I/O Restoration + K8s E2E Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore end-to-end Kinesis ingress/egress support in Kzmlabs StateFun by wiring the existing SDK specs (`statefun-kinesis-io`) to Flink 2.x `KinesisStreamsSource`/`KinesisStreamsSink` inside `statefun-flink-io-bundle`, add unit tests mirroring the Kafka pattern, add a new K8s-native E2E test module that validates the flow against a LocalStack Kinesis mock deployed in kind, and cut release `3.4.0-KZM-3.0-RC1`.

**Architecture:**
- **SDK layer** (unchanged) — `statefun-kinesis-io/` already ships `KinesisIngressBuilder`, `KinesisEgressBuilder`, spec classes, `AwsCredentials`, `AwsRegion`, etc. Users declare ingresses/egresses through these.
- **Runtime layer** (to restore) — `statefun-flink-io-bundle/src/main/java/.../flink/io/kinesis/` gets a new `KinesisFlinkIoModule` registered via `@AutoService(FlinkIoModule.class)`. It binds `KinesisSourceProvider` and `KinesisSinkProvider` for the `KinesisIOTypes` ingress/egress type names. Providers translate SDK `KinesisIngressSpec`/`KinesisEgressSpec` into `KinesisStreamsSource`/`KinesisStreamsSink` builders, mapping `AwsCredentials`/`AwsRegion`/`KinesisIngressStartupPosition` to `AWSConfigConstants` + `KinesisSourceConfigOptions`.
- **JSON v1 binders** — restored to parse `io.statefun.kinesis.v1/ingress` and `io.statefun.kinesis.v1/egress` kinds from `module.yaml`.
- **E2E layer** — new sibling module `statefun-k8s-native-kinesis-e2e/` mirrors `statefun-k8s-native-e2e/`'s structure but swaps Kafka for LocalStack Kinesis. Skippable via `-Dskip.kinesis.e2e`.
- **Release** — version bump `3.4.0-KZM-2.0` → `3.4.0-KZM-3.0-RC1` across 35 modules (34 existing + 1 new E2E), CHANGELOG entry, tag & release pipeline.

**Tech Stack:**
- Flink 2.2.0, Java 21
- `org.apache.flink:flink-connector-aws-kinesis-streams:6.0.0-2.0` (Flink 2.x compatible Kinesis connector, Source V2 / Sink V2 API)
- `org.apache.flink:flink-connector-aws-base:6.0.0-2.0` (transitive — provides `AWSConfigConstants`)
- LocalStack 4.x (Kinesis mock in kind)
- AWS SDK v2 (`software.amazon.awssdk:kinesis` — transitive)
- Existing: JUnit Jupiter 5.11.4, Hamcrest 3.0, AssertJ 3.27.3, awaitility 4.2.2
- Build: Maven, spotless (Google Java Format), maven-shade 3.6.1

**Research artifacts** (consult these if a task needs more background than the plan inlines):
- `tool-results/toolu_01X1xiCqTtq1yWVBF2yS7wi1.json` — Kafka runtime wiring full content (pattern to mirror)
- `tool-results/toolu_01XPHvGBiHwMVLNYWgeraZJm.json` — pre-deletion Kinesis runtime files (commit `01565664~1`, reference for logic porting)
- Run `git show 01565664~1:<path>` for any of the 16 deleted files to see pre-deletion source directly

---

## Phase 0 — Preflight

### Task 0.1: Create feature branch

- [ ] **Step 1: Confirm clean working tree on `release`**

Run: `git status && git log -1 --oneline`
Expected: clean tree, HEAD at `360adb34` or newer.

- [ ] **Step 2: Create feature branch**

Run: `git checkout -b feature/kinesis-restore`

- [ ] **Step 3: Verify we are on the new branch**

Run: `git branch --show-current`
Expected: `feature/kinesis-restore`

### Task 0.2: Verify the Flink 2.x Kinesis connector is reachable

- [ ] **Step 1: Probe Maven Central for the connector**

Run:
```
mvn dependency:get -Dartifact=org.apache.flink:flink-connector-aws-kinesis-streams:6.0.0-2.0
mvn dependency:get -Dartifact=org.apache.flink:flink-connector-aws-base:6.0.0-2.0
```
Expected: both BUILD SUCCESS; jars in `~/.m2/repository/org/apache/flink/`.

If either fails, the plan is blocked — stop and report.

### Task 0.3: Read research artifacts into head

- [ ] **Step 1: Read the Kafka-pattern reference**

Read `tool-results/toolu_01X1xiCqTtq1yWVBF2yS7wi1.json` fully. Focus on:
- `KafkaFlinkIoModule.java`
- `KafkaSourceProvider.java` (spec → `KafkaSource` builder)
- `KafkaSinkProvider.java` (spec → `KafkaSink` builder)
- `Kafka{Deserialization,Serialization}SchemaDelegate.java`
- `binders/ingress/v1/RoutableKafkaIngressBinderV1.java` + `Module.java`
- `binders/egress/v1/GenericKafkaEgressBinderV1.java` + `Module.java`

- [ ] **Step 2: Read the pre-deletion Kinesis reference**

Read `tool-results/toolu_01XPHvGBiHwMVLNYWgeraZJm.json` fully. These files are the pre-Flink-2.x implementation. Use them as a guide for **business logic** (e.g. how `AwsCredentials` mapped to connector properties), but do **not** copy the connector calls verbatim — the Flink 2.x connector has a different API.

---

## Phase 1 — Runtime wiring (SDK spec → Flink 2.x connector)

File structure under `statefun-flink/statefun-flink-io-bundle/src/main/java/org/apache/flink/statefun/flink/io/kinesis/`:

```
KinesisFlinkIoModule.java                    # ServiceLoader registration
KinesisSourceProvider.java                   # Spec → KinesisStreamsSource
KinesisSinkProvider.java                     # Spec → KinesisStreamsSink
KinesisDeserializationSchemaDelegate.java    # Wraps user KinesisIngressDeserializer
KinesisSerializationSchemaDelegate.java      # Wraps user KinesisEgressSerializer
AwsConfigAppender.java                       # Shared AwsCredentials + AwsRegion → AWSConfigConstants mapping (used by both source & sink)
binders/
  AwsCredentialsJsonDeserializer.java
  AwsRegionJsonDeserializer.java
  ingress/v1/
    Module.java
    RoutableKinesisIngressBinderV1.java
    RoutableKinesisIngressDeserializer.java
  egress/v1/
    Module.java
    GenericKinesisEgressBinderV1.java
    GenericKinesisEgressSpec.java
    GenericKinesisEgressSerializer.java
```

Tests under `statefun-flink/statefun-flink-io-bundle/src/test/java/org/apache/flink/statefun/flink/io/kinesis/`:

```
AwsConfigAppenderTest.java
KinesisSourceProviderTest.java
KinesisSinkProviderTest.java
KinesisDeserializationSchemaDelegateTest.java
KinesisSerializationSchemaDelegateTest.java
binders/ingress/v1/RoutableKinesisIngressBinderV1Test.java
binders/egress/v1/GenericKinesisEgressBinderV1Test.java
```

### Task 1.1: Add connector dependency to `statefun-flink-io-bundle`

**Files:**
- Modify: `statefun-flink/statefun-flink-io-bundle/pom.xml`
- Modify: `pom.xml` (root) — add version property
- Modify: `statefun-bom/pom.xml` — declare managed dependency

- [ ] **Step 1: Add `<flink.connector.aws.version>` property to root `pom.xml`**

In `pom.xml` under `<properties>`, next to `<flink.connector.kafka.version>`:

```xml
<flink.connector.aws.version>6.0.0-2.0</flink.connector.aws.version>
```

- [ ] **Step 2: Add managed dependency in `statefun-bom/pom.xml`**

Inside `<dependencyManagement><dependencies>`:

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-aws-kinesis-streams</artifactId>
  <version>${flink.connector.aws.version}</version>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-aws-base</artifactId>
  <version>${flink.connector.aws.version}</version>
</dependency>
```

- [ ] **Step 3: Add `statefun-kinesis-io` + connector deps to `statefun-flink-io-bundle/pom.xml`**

Add to `<dependencies>` (mirror the existing Kafka block):

```xml
<dependency>
  <groupId>io.github.kzmlabs.flinkstatefun</groupId>
  <artifactId>statefun-kinesis-io</artifactId>
  <version>${project.version}</version>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-aws-kinesis-streams</artifactId>
</dependency>
```

- [ ] **Step 4: Compile to verify dependency graph resolves**

Run: `mvn install -DskipTests -pl statefun-bom,statefun-flink/statefun-flink-io-bundle -am -B`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add pom.xml statefun-bom/pom.xml statefun-flink/statefun-flink-io-bundle/pom.xml
git commit -m "feat(kinesis): add flink-connector-aws-kinesis-streams 6.0.0-2.0 dep"
```

### Task 1.2: `AwsConfigAppender` — shared AWS config mapping

This is the shared helper that maps our SDK `AwsCredentials` + `AwsRegion` + startup position into `AWSConfigConstants` property names, used by both source and sink. The pre-deletion code had `AwsAuthConfigProperties.java` playing a similar role, but with the old connector property keys. We're rewriting it for the new connector.

**Files:**
- Create: `statefun-flink/statefun-flink-io-bundle/src/main/java/org/apache/flink/statefun/flink/io/kinesis/AwsConfigAppender.java`
- Test: `statefun-flink/statefun-flink-io-bundle/src/test/java/org/apache/flink/statefun/flink/io/kinesis/AwsConfigAppenderTest.java`

- [ ] **Step 1: Write the failing test**

```java
package org.apache.flink.statefun.flink.io.kinesis;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;
import org.junit.jupiter.api.Test;

class AwsConfigAppenderTest {

  @Test
  void appendsBasicCredentials() {
    Properties props = new Properties();
    AwsConfigAppender.appendCredentials(
        props, AwsCredentials.basic("AKIAFAKE", "secret/FAKE"));

    assertThat(props).containsEntry(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
    assertThat(props).containsEntry(AWSConfigConstants.accessKeyId(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER), "AKIAFAKE");
    assertThat(props).containsEntry(AWSConfigConstants.secretKey(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER), "secret/FAKE");
  }

  @Test
  void appendsDefaultProviderChain() {
    Properties props = new Properties();
    AwsConfigAppender.appendCredentials(props, AwsCredentials.fromDefaultProviderChain());
    assertThat(props).containsEntry(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");
  }

  @Test
  void appendsProfile() {
    Properties props = new Properties();
    AwsConfigAppender.appendCredentials(props, AwsCredentials.profile("myprofile"));
    assertThat(props).containsEntry(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "PROFILE");
    assertThat(props).containsKey(AWSConfigConstants.profileName(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER));
  }

  @Test
  void appendsDefaultRegion() {
    Properties props = new Properties();
    AwsConfigAppender.appendRegion(props, AwsRegion.fromDefaultProviderChain());
    assertThat(props).doesNotContainKey(AWSConfigConstants.AWS_REGION);
    assertThat(props).doesNotContainKey(AWSConfigConstants.AWS_ENDPOINT);
  }

  @Test
  void appendsSpecificRegion() {
    Properties props = new Properties();
    AwsConfigAppender.appendRegion(props, AwsRegion.ofId("us-east-1"));
    assertThat(props).containsEntry(AWSConfigConstants.AWS_REGION, "us-east-1");
    assertThat(props).doesNotContainKey(AWSConfigConstants.AWS_ENDPOINT);
  }

  @Test
  void appendsCustomEndpoint() {
    Properties props = new Properties();
    AwsConfigAppender.appendRegion(
        props, AwsRegion.ofCustomEndpoint("http://localstack:4566", "us-east-1"));
    assertThat(props).containsEntry(AWSConfigConstants.AWS_REGION, "us-east-1");
    assertThat(props).containsEntry(AWSConfigConstants.AWS_ENDPOINT, "http://localstack:4566");
  }
}
```

- [ ] **Step 2: Run the test — expect FAIL (class missing)**

Run: `mvn -pl statefun-flink/statefun-flink-io-bundle -Dtest=AwsConfigAppenderTest test -B`
Expected: compilation error (class not found).

- [ ] **Step 3: Implement `AwsConfigAppender`**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.statefun.flink.io.kinesis;

import java.util.Properties;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;

final class AwsConfigAppender {

  private AwsConfigAppender() {}

  static void appendCredentials(Properties props, AwsCredentials credentials) {
    if (credentials.isDefault()) {
      props.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");
      return;
    }
    if (credentials.isBasic()) {
      props.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
      props.setProperty(
          AWSConfigConstants.accessKeyId(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
          credentials.asBasic().accessKeyId());
      props.setProperty(
          AWSConfigConstants.secretKey(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
          credentials.asBasic().secretAccessKey());
      return;
    }
    if (credentials.isProfile()) {
      props.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "PROFILE");
      props.setProperty(
          AWSConfigConstants.profileName(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
          credentials.asProfile().name());
      credentials
          .asProfile()
          .path()
          .ifPresent(
              p ->
                  props.setProperty(
                      AWSConfigConstants.profilePath(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
                      p));
      return;
    }
    throw new IllegalArgumentException("Unrecognized AwsCredentials variant: " + credentials);
  }

  static void appendRegion(Properties props, AwsRegion region) {
    if (region.isDefault()) {
      return;
    }
    if (region.isId()) {
      props.setProperty(AWSConfigConstants.AWS_REGION, region.asId().id());
      return;
    }
    if (region.isCustomEndpoint()) {
      AwsRegion.CustomEndpointAwsRegion r = region.asCustomEndpoint();
      props.setProperty(AWSConfigConstants.AWS_REGION, r.regionId());
      props.setProperty(AWSConfigConstants.AWS_ENDPOINT, r.endpoint());
      return;
    }
    throw new IllegalArgumentException("Unrecognized AwsRegion variant: " + region);
  }
}
```

**Caveat:** The SDK's `AwsCredentials`/`AwsRegion` expose `isBasic()`, `isProfile()`, `isDefault()`, `asBasic()`, `asCustomEndpoint()` etc. methods. Open `statefun-kinesis-io/src/main/java/org/apache/flink/statefun/sdk/kinesis/auth/AwsCredentials.java` to confirm exact method names; adjust the test + implementation consistently. If the SDK uses a visitor pattern, rewrite the `if` chain using the visitor.

- [ ] **Step 4: Run tests — expect PASS**

Run: `mvn -pl statefun-flink/statefun-flink-io-bundle -Dtest=AwsConfigAppenderTest test -B`
Expected: Tests run: 6, Failures: 0, Errors: 0.

- [ ] **Step 5: Commit**

```bash
git add statefun-flink/statefun-flink-io-bundle/src/main/java/org/apache/flink/statefun/flink/io/kinesis/AwsConfigAppender.java
git add statefun-flink/statefun-flink-io-bundle/src/test/java/org/apache/flink/statefun/flink/io/kinesis/AwsConfigAppenderTest.java
git commit -m "feat(kinesis): AwsConfigAppender maps SDK creds/region to AWSConfigConstants"
```

### Task 1.3: `KinesisDeserializationSchemaDelegate`

Wraps a user-provided `KinesisIngressDeserializer` in a Flink `DeserializationSchema<Message>` (where `Message` is the StateFun internal message type).

**Files:**
- Create: `statefun-flink/statefun-flink-io-bundle/src/main/java/org/apache/flink/statefun/flink/io/kinesis/KinesisDeserializationSchemaDelegate.java`
- Test: `statefun-flink/statefun-flink-io-bundle/src/test/java/org/apache/flink/statefun/flink/io/kinesis/KinesisDeserializationSchemaDelegateTest.java`

Consult the pre-deletion `KinesisDeserializationSchemaDelegate.java` (via `git show 01565664~1:statefun-flink/statefun-flink-io-bundle/src/main/java/org/apache/flink/statefun/flink/io/kinesis/KinesisDeserializationSchemaDelegate.java`) for the existing shape — it should largely port as-is because `org.apache.flink.api.common.serialization.DeserializationSchema` is unchanged between 1.16 and 2.2. If the old code extended `KinesisDeserializationSchema`, switch to `DeserializationSchema<Message>` since the new Source V2 builder takes the plain variant (preferred) and Kinesis metadata is not needed by StateFun.

- [ ] **Step 1: Fetch the old file for reference**

Run: `git show 01565664~1:statefun-flink/statefun-flink-io-bundle/src/main/java/org/apache/flink/statefun/flink/io/kinesis/KinesisDeserializationSchemaDelegate.java`

- [ ] **Step 2: Write failing test** — round-trip a byte array through the delegate, assert the user-supplied deserializer is called with the right `IngressRecord` (stream name, partition key, seq number, data). Mirror `KafkaDeserializationSchemaDelegateTest.java` (see Kafka-pattern artifact).

- [ ] **Step 3: Run — FAIL (class missing)**

- [ ] **Step 4: Implement** — port the old class, keeping logic but returning `DeserializationSchema<Message>`. The input `byte[]` is the Kinesis record `Data`; stream name and partition key are not available through plain `DeserializationSchema`, so pass empty strings (consistent with old behavior which also only used data for routing decisions downstream).

- [ ] **Step 5: Run — PASS**

- [ ] **Step 6: Commit**

```bash
git commit -m "feat(kinesis): KinesisDeserializationSchemaDelegate for Source V2"
```

### Task 1.4: `KinesisSerializationSchemaDelegate`

Wraps a user-provided `KinesisEgressSerializer<Message>` as a Flink `SerializationSchema<Message>` **plus** a `PartitionKeyGenerator<Message>` for the sink builder.

**Files:**
- Create: `.../io/kinesis/KinesisSerializationSchemaDelegate.java`
- Test: `.../io/kinesis/KinesisSerializationSchemaDelegateTest.java`

Because `KinesisStreamsSink` takes a `SerializationSchema<T>` AND a separate `PartitionKeyGenerator<T>`, we need one delegate class that exposes both. Approach: the class itself is a `SerializationSchema<Message>` producing the record `Data`, and has a public `partitionKeyGenerator()` method returning the key extractor.

- [ ] **Step 1: Fetch old reference**

Run: `git show 01565664~1:statefun-flink/statefun-flink-io-bundle/src/main/java/org/apache/flink/statefun/flink/io/kinesis/CachingPartitionerSerializerDelegate.java`

The old code cached the serialized `EgressRecord` so the partition key and data both came from one serialization call. This optimization is still worth keeping — the Flink 2.x sink is batched so serialization happens once per record anyway, but caching avoids a second call for partition key extraction.

- [ ] **Step 2: Write test** — feed a fake `Message`, assert the delegate's `serialize(msg)` returns the `EgressRecord.data`, and `partitionKeyGenerator().apply(msg)` returns the same record's `partitionKey`. Use a thread-local cache OR a weak map; the old code used a `ThreadLocal<Map<Message, EgressRecord>>` that relied on the SinkFunction being single-threaded per subtask — verify the Sink V2 invocation model still gives us that guarantee (it does: the `SinkWriter` is called from a single task thread).

- [ ] **Step 3..6: Implement, test PASS, commit**

```bash
git commit -m "feat(kinesis): KinesisSerializationSchemaDelegate w/ cached partition key"
```

### Task 1.5: `KinesisSourceProvider` — spec → `KinesisStreamsSource`

**Files:**
- Create: `.../io/kinesis/KinesisSourceProvider.java`
- Test: `.../io/kinesis/KinesisSourceProviderTest.java`

- [ ] **Step 1: Write test**

```java
package org.apache.flink.statefun.flink.io.kinesis;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.statefun.flink.io.spi.JsonIngressSpec;
import org.apache.flink.statefun.sdk.kinesis.KinesisIOTypes;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressBuilder;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressSpec;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressStartupPosition;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.junit.jupiter.api.Test;

class KinesisSourceProviderTest {

  @Test
  void buildsSourceFromSpec() {
    KinesisIngressSpec<byte[]> spec =
        KinesisIngressBuilder.forIdentifier(
                new IngressIdentifier<>(byte[].class, "example", "kinesis-in"))
            .withDeserializer(new PassthroughIngressDeserializer())
            .withStream("events-in")
            .withAwsCredentials(AwsCredentials.basic("test", "test"))
            .withAwsRegion(AwsRegion.ofCustomEndpoint("http://localhost:4566", "us-east-1"))
            .withStartupPosition(KinesisIngressStartupPosition.fromLatest())
            .build();

    KinesisSourceProvider provider = new KinesisSourceProvider();
    // The SourceProvider interface returns a Source<?, ?, ?> or SourceFunction.
    // Assert the returned Source is non-null and is a KinesisStreamsSource instance.
    assertThat(provider.forSpec(new JsonIngressSpec<>(KinesisIOTypes.UNIVERSAL_INGRESS_TYPE, spec.id(), spec)))
        .isNotNull();
  }

  // ... more tests: TRIM_HORIZON, AT_TIMESTAMP, default provider chain, etc.
}
```

Note: `PassthroughIngressDeserializer` is a test fixture — create it as a private static class or a new test-scoped helper.

- [ ] **Step 2: Run — FAIL**

- [ ] **Step 3: Implement**

```java
package org.apache.flink.statefun.flink.io.kinesis;

import java.util.Properties;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressSpec;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressStartupPosition;

public final class KinesisSourceProvider implements SourceProvider {

  @Override
  public <T> org.apache.flink.api.connector.source.Source<T, ?, ?> forSpec(
      IngressSpec<T> ingressSpec) {
    KinesisIngressSpec<T> spec = asKinesisSpec(ingressSpec);

    Configuration sourceConfig = new Configuration();
    applyStartupPosition(sourceConfig, spec.startupPosition());

    Properties awsProps = new Properties();
    AwsConfigAppender.appendCredentials(awsProps, spec.awsCredentials());
    AwsConfigAppender.appendRegion(awsProps, spec.awsRegion());
    awsProps.forEach((k, v) -> sourceConfig.setString(k.toString(), v.toString()));

    return KinesisStreamsSource.<T>builder()
        .setStreamArn(buildStreamArn(spec))
        .setSourceConfig(sourceConfig)
        .setDeserializationSchema(new KinesisDeserializationSchemaDelegate<>(spec.deserializer()))
        .build();
  }

  private static <T> KinesisIngressSpec<T> asKinesisSpec(IngressSpec<T> ingressSpec) {
    if (ingressSpec instanceof KinesisIngressSpec) {
      return (KinesisIngressSpec<T>) ingressSpec;
    }
    throw new IllegalArgumentException(
        "Expected KinesisIngressSpec but was " + ingressSpec.getClass());
  }

  private static String buildStreamArn(KinesisIngressSpec<?> spec) {
    // Option A: spec exposes streamArn() directly. Add that method to the SDK.
    // Option B: compose ARN from region + account + stream name.
    // Prefer Option A — user-supplied ARN is more flexible.
    return spec.streamArn(); // Requires SDK extension — see Task 1.6
  }

  private static void applyStartupPosition(
      Configuration cfg, KinesisIngressStartupPosition position) {
    if (position.isLatest()) {
      cfg.set(
          KinesisSourceConfigOptions.STREAM_INITIAL_POSITION,
          KinesisSourceConfigOptions.InitialPosition.LATEST);
      return;
    }
    if (position.isEarliest()) {
      cfg.set(
          KinesisSourceConfigOptions.STREAM_INITIAL_POSITION,
          KinesisSourceConfigOptions.InitialPosition.TRIM_HORIZON);
      return;
    }
    if (position.isAtDate()) {
      cfg.set(
          KinesisSourceConfigOptions.STREAM_INITIAL_POSITION,
          KinesisSourceConfigOptions.InitialPosition.AT_TIMESTAMP);
      cfg.set(
          KinesisSourceConfigOptions.STREAM_INITIAL_TIMESTAMP,
          Double.toString(position.asDate().date().getTime() / 1000.0));
      return;
    }
    throw new IllegalArgumentException("Unrecognized startup position: " + position);
  }
}
```

- [ ] **Step 4: Run — PASS**

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(kinesis): KinesisSourceProvider targets Flink 2.x KinesisStreamsSource"
```

### Task 1.6: SDK extension — add `streamArn()` to `KinesisIngressSpec` / `KinesisEgressSpec` if missing

Check if the SDK `KinesisIngressSpec.java` and `KinesisEgressSpec.java` already expose stream ARN (Flink 2.x Kinesis source requires ARN not name). If only `streamName()` exists, either:

- **(A)** Add a `streamArn()` method + builder setter that defaults to ARN formula `arn:aws:kinesis:<region>:<account>:stream/<name>` when not set explicitly.
- **(B)** Derive in the provider when only name is set.

Option A is cleaner and future-proof.

- [ ] **Step 1: Read current SDK spec**

Run: `cat statefun-kinesis-io/src/main/java/org/apache/flink/statefun/sdk/kinesis/ingress/KinesisIngressSpec.java`

- [ ] **Step 2: If `streamArn()` exists, skip to next task. Otherwise add it**

Modify `KinesisIngressSpec.java` and `KinesisEgressSpec.java`:
- Add `private final String streamArn;`
- Add getter `public String streamArn()`
- Add builder method `.withStreamArn(String arn)` on both builders
- Keep `streamName` for backward compat; if only name is set, construct ARN in `build()` using region (requires region to be an `IdAwsRegion` — if it's `fromDefaultProviderChain()` or `CustomEndpointAwsRegion`, we can't build a correct ARN automatically; throw in that case asking the user to set `streamArn` explicitly).

- [ ] **Step 3: Unit tests for ARN construction**

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(kinesis-sdk): expose streamArn() on Ingress/Egress specs"
```

### Task 1.7: `KinesisSinkProvider` — spec → `KinesisStreamsSink`

**Files:**
- Create: `.../io/kinesis/KinesisSinkProvider.java`
- Test: `.../io/kinesis/KinesisSinkProviderTest.java`

Similar structure to source provider but use `KinesisStreamsSink.<T>builder()`, set stream **name** (not ARN — the sink API takes name), set `KinesisClientProperties` (Properties instance, NOT Configuration), set serialization schema + partition key generator both from the `KinesisSerializationSchemaDelegate`.

- [ ] **Step 1..6: TDD as above**

```bash
git commit -m "feat(kinesis): KinesisSinkProvider targets Flink 2.x KinesisStreamsSink"
```

### Task 1.8: `KinesisFlinkIoModule` — ServiceLoader registration

**Files:**
- Create: `.../io/kinesis/KinesisFlinkIoModule.java`

Note naming: the pre-deletion file was `KinesisFlinkIOModule` (uppercase IO). New Kafka file is `KafkaFlinkIoModule` (lowercase Io). **Use `KinesisFlinkIoModule`** to match the current Kafka convention.

- [ ] **Step 1: Implement**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.statefun.flink.io.kinesis;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.statefun.flink.io.spi.FlinkIoModule;
import org.apache.flink.statefun.sdk.kinesis.KinesisIOTypes;

@AutoService(FlinkIoModule.class)
public final class KinesisFlinkIoModule implements FlinkIoModule {

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    binder.bindSourceProvider(KinesisIOTypes.UNIVERSAL_INGRESS_TYPE, new KinesisSourceProvider());
    binder.bindSinkProvider(KinesisIOTypes.UNIVERSAL_EGRESS_TYPE, new KinesisSinkProvider());
  }
}
```

- [ ] **Step 2: Build & confirm META-INF/services is generated**

Run: `mvn install -DskipTests -pl statefun-flink/statefun-flink-io-bundle -B`
Expected: `target/classes/META-INF/services/org.apache.flink.statefun.flink.io.spi.FlinkIoModule` contains both `KafkaFlinkIoModule` and `KinesisFlinkIoModule`.

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(kinesis): register KinesisFlinkIoModule via ServiceLoader"
```

### Task 1.9: JSON v1 binders (for `module.yaml` parsing)

These handle the `kind: io.statefun.kinesis.v1/ingress` / `egress` entries in `module.yaml`. Pre-deletion code exists for these — port with minimal changes because the JSON format is a user contract we must preserve.

**Files:**
- Create: `.../io/kinesis/binders/AwsCredentialsJsonDeserializer.java`
- Create: `.../io/kinesis/binders/AwsRegionJsonDeserializer.java`
- Create: `.../io/kinesis/binders/ingress/v1/Module.java`
- Create: `.../io/kinesis/binders/ingress/v1/RoutableKinesisIngressBinderV1.java`
- Create: `.../io/kinesis/binders/ingress/v1/RoutableKinesisIngressDeserializer.java`
- Create: `.../io/kinesis/binders/egress/v1/Module.java`
- Create: `.../io/kinesis/binders/egress/v1/GenericKinesisEgressBinderV1.java`
- Create: `.../io/kinesis/binders/egress/v1/GenericKinesisEgressSpec.java`
- Create: `.../io/kinesis/binders/egress/v1/GenericKinesisEgressSerializer.java`
- Test: `.../io/kinesis/binders/ingress/v1/RoutableKinesisIngressBinderV1Test.java` (mirror `RoutableKafkaIngressBinderV1Test.java`)
- Test: `.../io/kinesis/binders/egress/v1/GenericKinesisEgressBinderV1Test.java` (mirror `GenericKafkaEgressBinderV1Test.java`)
- Test resources: `src/test/resources/kinesis-io-binders/*.yaml` (small YAML fixtures)

- [ ] **Step 1: Port deserializers from pre-deletion source**

For each of the 9 binder files, run `git show 01565664~1:<path>` and port. The `AwsCredentialsJsonDeserializer` / `AwsRegionJsonDeserializer` are pure JSON parsers — port as-is.

- [ ] **Step 2: Write YAML fixtures for tests** — create one minimal ingress YAML and one egress YAML covering: each AwsCredentials variant, each AwsRegion variant, each startup position.

- [ ] **Step 3: Mirror the Kafka binder tests** — assert parsing each fixture produces the expected `KinesisIngressSpec` / `KinesisEgressSpec`.

- [ ] **Step 4: Run tests — PASS**

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(kinesis): restore JSON v1 binders for module.yaml integration"
```

### Task 1.10: End-of-phase smoke — full module build + tests

- [ ] **Step 1: Run all tests in the io-bundle module**

Run: `mvn install -pl statefun-flink/statefun-flink-io-bundle -am -B`
Expected: BUILD SUCCESS, all Kinesis tests pass, all pre-existing Kafka tests still pass.

- [ ] **Step 2: Spotless check**

Run: `mvn spotless:check -pl statefun-flink/statefun-flink-io-bundle -B`
If fails: `mvn spotless:apply -pl statefun-flink/statefun-flink-io-bundle` and amend.

- [ ] **Step 3: RAT check**

Run: `mvn apache-rat:check -pl statefun-flink/statefun-flink-io-bundle -B`
Expected: PASS (every new file has the ASL header).

- [ ] **Step 4: Commit any fixups, push branch for early review**

```bash
git push -u origin feature/kinesis-restore
```

---

## Phase 2 — K8s native E2E module (LocalStack-backed)

New module `statefun-k8s-native-kinesis-e2e/`, sibling to `statefun-k8s-native-e2e/`. Reuses the same remote-function image for CounterFn (adapted to read from Kinesis) + adds LocalStack service deployment.

### Task 2.1: Scaffold new module

**Files:**
- Create: `statefun-k8s-native-kinesis-e2e/pom.xml`
- Modify: `pom.xml` (root) — add `<module>statefun-k8s-native-kinesis-e2e</module>`
- Modify: `.github/workflows/release.yml` — ensure new module is in the exclusion list for Maven Central deploy (it's an E2E module, not published)

- [ ] **Step 1: Create `pom.xml`** — copy `statefun-k8s-native-e2e/pom.xml` verbatim, change `<artifactId>` to `statefun-k8s-native-kinesis-e2e`, **change profile activation** from `!skip.k8s.e2e` to `!skip.kinesis.e2e`, remove Kafka deps, add AWS SDK v2 Kinesis client for test-side production/consumption:

```xml
<dependency>
  <groupId>software.amazon.awssdk</groupId>
  <artifactId>kinesis</artifactId>
  <version>2.29.29</version>
  <scope>test</scope>
</dependency>
```

(Keep `minio` dep for checkpoint verification; keep `awaitility`, `assertj`, `junit-jupiter`, `slf4j-simple`.)

- [ ] **Step 2: Add to root reactor**

In root `pom.xml`, after `<module>statefun-k8s-native-e2e</module>`:
```xml
<module>statefun-k8s-native-kinesis-e2e</module>
```

- [ ] **Step 3: Ensure release.yml excludes this module from Maven Central deploy**

Inspect `.github/workflows/release.yml` line ~87 (the `-pl '!...'` list) and add `!statefun-k8s-native-kinesis-e2e`.

- [ ] **Step 4: Verify reactor resolves**

Run: `mvn validate -B`
Expected: BUILD SUCCESS, new module visible in reactor list.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(kinesis-e2e): scaffold statefun-k8s-native-kinesis-e2e module"
```

### Task 2.2: Teardown and setup cluster scripts

**Files:**
- Create: `statefun-k8s-native-kinesis-e2e/scripts/setup-cluster.sh`
- Create: `statefun-k8s-native-kinesis-e2e/scripts/teardown-cluster.sh`

- [ ] **Step 1: Copy `setup-cluster.sh` from `statefun-k8s-native-e2e` and modify:**
  - Cluster name: `statefun-kinesis-e2e` (distinct so both suites can coexist in CI)
  - Replace Kafka deployment with LocalStack deployment
  - Remove `Create Kafka topics` step; replace with `Create Kinesis streams` via `awslocal kinesis create-stream --stream-name commands --shard-count 1` (etc.)
  - LocalStack exposes AWS APIs on port 4566

- [ ] **Step 2: Teardown script** — identical to Kafka one but with the new cluster name.

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(kinesis-e2e): setup/teardown scripts (kind + LocalStack)"
```

### Task 2.3: K8s manifests

**Files:**
- Create: `statefun-k8s-native-kinesis-e2e/src/test/resources/k8s/namespace.yaml`
- Create: `statefun-k8s-native-kinesis-e2e/src/test/resources/k8s/flink-rbac.yaml`
- Create: `statefun-k8s-native-kinesis-e2e/src/test/resources/k8s/localstack.yaml`
- Create: `statefun-k8s-native-kinesis-e2e/src/test/resources/k8s/minio.yaml` (identical to Kafka E2E)
- Create: `statefun-k8s-native-kinesis-e2e/src/test/resources/k8s/remote-function.yaml`
- Create: `statefun-k8s-native-kinesis-e2e/src/test/resources/k8s/module-configmap.yaml`
- Create: `statefun-k8s-native-kinesis-e2e/src/test/resources/k8s/flink-deployment.yaml`

- [ ] **Step 1: `namespace.yaml`** — namespace `statefun-kinesis-e2e`

- [ ] **Step 2: `localstack.yaml`** — single-replica Deployment + Service:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: localstack
  namespace: statefun-kinesis-e2e
spec:
  replicas: 1
  selector:
    matchLabels: { app: localstack }
  template:
    metadata:
      labels: { app: localstack }
    spec:
      containers:
        - name: localstack
          image: localstack/localstack:4.1
          ports:
            - containerPort: 4566
          env:
            - name: SERVICES
              value: kinesis
            - name: DEFAULT_REGION
              value: us-east-1
            - name: DEBUG
              value: "0"
          readinessProbe:
            httpGet:
              path: /_localstack/health
              port: 4566
            initialDelaySeconds: 5
            periodSeconds: 3
---
apiVersion: v1
kind: Service
metadata:
  name: localstack
  namespace: statefun-kinesis-e2e
spec:
  selector: { app: localstack }
  ports:
    - port: 4566
      targetPort: 4566
```

- [ ] **Step 3: `module-configmap.yaml`** — mirror Kafka E2E's, but with Kinesis kinds:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: statefun-kinesis-e2e-module
  namespace: statefun-kinesis-e2e
data:
  module.yaml: |
    version: "3.0"
    module:
      meta:
        type: remote
      spec:
        endpoints:
          - endpoint:
              meta:
                kind: http
              spec:
                functions: e2e.k8s/*
                urlPathTemplate: http://remote-function.statefun-kinesis-e2e:8080/statefun
                transport:
                  type: io.statefun.transports.v1/async
                  call: 30s
        ingresses:
          - ingress:
              meta:
                type: io.statefun.kinesis.v1/ingress
                id: e2e/counter-in
              spec:
                streamArn: arn:aws:kinesis:us-east-1:000000000000:stream/commands
                awsRegion:
                  type: custom-endpoint
                  endpoint: http://localstack.statefun-kinesis-e2e:4566
                  id: us-east-1
                awsCredentials:
                  type: basic
                  accessKeyId: test
                  secretAccessKey: test
                startupPosition:
                  type: earliest
                targets:
                  - e2e.k8s/counter
                valueType: io.github.kzmlabs.statefun.e2e/CounterCommand
        egresses:
          - egress:
              meta:
                type: io.statefun.kinesis.v1/egress
                id: e2e/results
              spec:
                streamName: results
                awsRegion:
                  type: custom-endpoint
                  endpoint: http://localstack.statefun-kinesis-e2e:4566
                  id: us-east-1
                awsCredentials:
                  type: basic
                  accessKeyId: test
                  secretAccessKey: test
```

- [ ] **Step 4: Other manifests** — copy the rest from Kafka E2E mutatis mutandis: update namespace to `statefun-kinesis-e2e`, update `flink-job-name` to `statefun-kinesis-e2e`, the `FlinkDeployment` spec keeps RocksDB + MinIO checkpoint storage unchanged.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(kinesis-e2e): K8s manifests w/ LocalStack + Kinesis module.yaml"
```

### Task 2.4: Remote-function module for Kinesis E2E

**Decision:** We could reuse the existing `statefun-k8s-native-e2e/remote-function/` module directly (the `CounterFn` logic is transport-agnostic — it doesn't know whether it came from Kafka or Kinesis). This saves ~300 LOC of duplication.

- [ ] **Step 1: In `statefun-k8s-native-kinesis-e2e/pom.xml`, add a `<dependency>` on the Kafka E2E's `remote-function` module**

Actually that module is not a Maven artifact — it's a nested build. Easier path: **reuse the built Docker image** `statefun-remote-function:e2e` that the Kafka E2E produces. The Kinesis E2E pom can declare a dependency on `statefun-k8s-native-e2e` having been built first by adding it to the `<executions>` in the exec-plugin (build-remote-function-image phase becomes a no-op if image already exists, or shell out to check and conditionally build).

- [ ] **Step 2: Adjust `statefun-k8s-native-kinesis-e2e/pom.xml`'s `exec-maven-plugin`** to remove the `build-remote-function` and `build-remote-function-image` executions, rely on the image being built by the Kafka E2E module running earlier in the reactor.

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(kinesis-e2e): reuse remote-function image from Kafka E2E"
```

### Task 2.5: `StateFunKinesisK8sE2E` test class

**Files:**
- Create: `statefun-k8s-native-kinesis-e2e/src/test/java/org/apache/flink/statefun/e2e/kinesis/k8s/StateFunKinesisK8sE2E.java`
- Create: `statefun-k8s-native-kinesis-e2e/src/test/proto/e2e.proto` (or reuse via Maven)

- [ ] **Step 1: Define a test that:**
  - Port-forwards LocalStack service: `kubectl port-forward -n statefun-kinesis-e2e svc/localstack 4566:4566`
  - Uses AWS SDK v2 `KinesisClient` pointed at `http://localhost:4566` to put `CounterCommand` records into the `commands` stream
  - Polls the `results` stream via SDK, decodes `CounterResult` protobufs, asserts state accumulates correctly via awaitility (mirror `protobufCounterFunction_sumsDeltasCorrectly` from Kafka E2E)
  - Verifies checkpoints appear in MinIO (mirror the third Kafka test)

- [ ] **Step 2: Implement** (see Kafka test as template in research artifact; swap `KafkaProducer`/`KafkaConsumer` for AWS SDK v2 `KinesisClient.putRecord` / `KinesisClient.getRecords` loops)

- [ ] **Step 3: Commit**

```bash
git commit -m "test(kinesis-e2e): StateFunKinesisK8sE2E integration test"
```

### Task 2.6: CI workflow — kinesis E2E job

**Files:**
- Modify: `.github/workflows/e2e-test.yml` — add a second job `kinesis-e2e` alongside `k8s-e2e`

- [ ] **Step 1: Copy the existing `k8s-e2e` job** under a new id `kinesis-e2e`. Change the Maven target to `mvn verify -pl statefun-k8s-native-kinesis-e2e -Dskip.k8s.e2e -B` so it runs the Kinesis E2E while skipping the Kafka one (they use different cluster names but can conflict on shared ports).

- [ ] **Step 2: Update `release.yml` and `docker-release.yml`** to gate on both jobs.

- [ ] **Step 3: Commit**

```bash
git commit -m "ci: add kinesis-e2e job alongside k8s-e2e"
```

### Task 2.7: Local validation

- [ ] **Step 1: Run the new E2E locally**

Run: `mvn verify -pl statefun-k8s-native-kinesis-e2e -Dskip.k8s.e2e -B`
Expected: BUILD SUCCESS. Expected runtime: ~10 minutes (LocalStack + Flink + StateFun).

If tests fail, use `kubectl logs -n statefun-kinesis-e2e` + `kubectl describe flinkdeployment/statefun-jobmanager` to diagnose.

- [ ] **Step 2: Verify skip flag works**

Run: `mvn verify -pl statefun-k8s-native-kinesis-e2e -Dskip.kinesis.e2e -B`
Expected: BUILD SUCCESS, no cluster created, no tests run.

- [ ] **Step 3: Verify full reactor still passes**

Run: `mvn install -B -Drat.skip=false` (takes ~45 min now — both K8s E2Es run)
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit any fixes**

---

## Phase 3 — Release 3.4.0-KZM-3.0-RC1

### Task 3.1: Version bump

- [ ] **Step 1: Bump pom version**

Run: `mvn versions:set -DnewVersion=3.4.0-KZM-3.0-RC1 -DgenerateBackupPoms=false -B`

- [ ] **Step 2: Populate .m2 with new version**

Run: `mvn install -DskipTests -Drat.skip=true -B`
Expected: BUILD SUCCESS.

### Task 3.2: Docs update

- [ ] **Step 1: Update README.md**
  - Bump `-RC7` → `-KZM-3.0-RC1` in Maven example blocks
  - In the "Differences from Apache StateFun" table, change `Kinesis Connector | Included | Removed` to `Kinesis Connector | Included | Included (Flink 2.x Source V2 + LocalStack E2E)`
  - Add a short "Kinesis" subsection under "Module Structure" briefly explaining SDK + runtime wiring.

- [ ] **Step 2: Update RELEASE-GUIDE.md**
  - Current Release → `3.4.0-KZM-3.0-RC1`

- [ ] **Step 3: Update tools/docker/build-stateful-functions.sh**
  - `VERSION_TAG` default → `3.4.0-KZM-3.0-RC1`

- [ ] **Step 4: Update CHANGELOG.md**

Add new entry at top:

```markdown
## [3.4.0-KZM-3.0-RC1] - 2026-04-23

### Added
- **Kinesis ingress/egress runtime** — Restored end-to-end Kinesis support on Flink 2.x via `flink-connector-aws-kinesis-streams:6.0.0-2.0` (Source V2 / Sink V2). SDK specs in `statefun-kinesis-io` are now backed by runtime providers in `statefun-flink-io-bundle`; `module.yaml` entries with `io.statefun.kinesis.v1/ingress` and `/egress` work again.
- **LocalStack-backed Kinesis K8s E2E** — New `statefun-k8s-native-kinesis-e2e` module spins up kind + Flink Kubernetes Operator 1.11 + LocalStack 4.1 + MinIO and runs a protobuf counter flow end-to-end. Skippable via `-Dskip.kinesis.e2e`.

### Fixed
- Closes the regression introduced in 3.4.0-KZM-2.0 where `statefun-kinesis-io` shipped SDK-only with no runtime wiring (16 files had been deleted during the Flink 2.x migration in commit `01565664`).
```

Plus the link ref at the bottom.

- [ ] **Step 5: Commit**

```bash
git add -u
git commit -m "Release 3.4.0-KZM-3.0-RC1"
```

### Task 3.3: Final full build + PR

- [ ] **Step 1: Full build with all tests**

Run: `mvn clean install -B -Drat.skip=false`
Expected: BUILD SUCCESS — 35 modules, both K8s E2Es green.

- [ ] **Step 2: Push branch**

Run: `git push -u origin feature/kinesis-restore`

- [ ] **Step 3: Open PR**

Run:
```bash
gh pr create --repo kzmlabs/flink-statefun --base release --head feature/kinesis-restore --title "Release 3.4.0-KZM-3.0-RC1: Kinesis I/O restored + K8s E2E" --body "$(cat <<'EOF'
## Summary

Restores Kinesis ingress/egress end-to-end support on Flink 2.x and ships it behind the new `3.4.0-KZM-3.0-RC1` release candidate.

... (expand per PR template)
EOF
)"
```

- [ ] **Step 4: Watch CI, admin-merge when green, tag `v3.4.0-KZM-3.0-RC1`** (follow the same tag-push → release-pipeline flow used for RC7).

---

## Phase 4 — Self-review checkpoints (inline during execution)

After each phase, before committing the last task, re-check:

1. **Code compiles cleanly**: `mvn install -DskipTests -pl <module> -am -B`
2. **Tests pass**: `mvn test -pl <module> -am -B`
3. **Spotless clean**: `mvn spotless:check -pl <module> -B`
4. **RAT clean**: `mvn apache-rat:check -pl <module> -B`
5. **No unintended file changes**: `git status`

---

## Open questions the engineer should surface before starting Phase 2

1. **SDK API surface** — does `KinesisIngressSpec` currently expose a field the new connector needs (stream ARN)? If not, is adding it considered a **breaking change** for existing SDK users? (Answer: no — additive method, backward-compatible.) If the engineer finds uncertainty, stop and consult.
2. **LocalStack edition** — confirm the `localstack/localstack:4.1` free (community) edition covers Kinesis. (Answer: yes, Kinesis is in the free tier.)
3. **CI runtime** — adding a second K8s E2E doubles the release-pipeline duration from ~20min to ~35min. Acceptable for a stable release gate?

---

**End of plan.**
