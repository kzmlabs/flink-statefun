# Kafka Invalid-Record E2E Scenarios Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Isolated K8s-native E2E scenarios proving that invalid Kafka records (null key, tombstone) fail the routable ingress with full record coordinates in the diagnostics, extensible to future `invalidRecordHandling` strategies.

**Architecture:** A second, minimal FlinkDeployment (`statefun-e2e-invalid`) with its own module ConfigMap and dedicated topics, so poison pills never touch the main suite's job. One plain test method per scenario in a new `StateFunKafkaInvalidRecordsE2E` class; a tiny `Kubectl` util (ProcessBuilder, same pattern as `KubectlPortForward`) reads CR state and JobManager logs and redeploys between fatal scenarios.

**Tech Stack:** JUnit 5 + AssertJ + Awaitility (existing), kubectl via ProcessBuilder, Flink Kubernetes Operator CRs, kind.

## Global Constraints

- English only in all repo artifacts (code, comments, commit messages).
- Java style: no line wrapping to ~100 chars; keep statements on one line; short HTML-free Javadoc instead of `//` comments for method contracts.
- No AI attribution anywhere in commits.
- Branch: `fix/kafka-ingress-invalid-record-context` (continue on it).
- The fatal-poison redeploy MUST also delete+recreate the poisoned topic: the uncommitted poison record would otherwise kill the redeployed job again (same consumer group, `startupPosition: earliest`).
- Do not modify the main `flink-deployment.yaml`, `module-configmap.yaml`, or existing test classes.

---

### Task 1: Manifests for the isolated invalid-records deployment

**Files:**
- Create: `statefun-e2e-tests/statefun-e2e-k8s-native/src/test/resources/k8s/module-configmap-invalid.yaml`
- Create: `statefun-e2e-tests/statefun-e2e-k8s-native/src/test/resources/k8s/flink-deployment-invalid.yaml`

**Interfaces:**
- Produces: FlinkDeployment CR named `statefun-e2e-invalid` in namespace `statefun-e2e`; ingress topic `invalid.commands` (valueType `io.github.kzmlabs.statefun.e2e/CounterCommand`, target `counter.kafka/fn`); consumer group `statefun-e2e-invalid`; restart strategy fixed-delay with 1 attempt so a poison record reaches terminal FAILED within seconds.

- [ ] **Step 1: Write `module-configmap-invalid.yaml`**

```yaml
# SPDX-License-Identifier: Apache-2.0


apiVersion: v1
kind: ConfigMap
metadata:
  name: statefun-e2e-module-invalid
  namespace: statefun-e2e
data:
  module.yaml: |
    ---
    kind: io.statefun.endpoints.v2/http
    spec:
      functions: counter.kafka/*
      urlPathTemplate: http://remote-function.statefun-e2e:8080/statefun
      transport:
        type: io.statefun.transports.v1/async
        timeouts:
          call: 30sec
          connect: 10sec
    ---
    kind: io.statefun.kafka.v1/ingress
    spec:
      id: invalid/kafka-in
      address: kafka.statefun-e2e:9092
      consumerGroupId: statefun-e2e-invalid
      startupPosition:
        type: earliest
      topics:
        - topic: invalid.commands
          valueType: io.github.kzmlabs.statefun.e2e/CounterCommand
          targets:
            - counter.kafka/fn
    ---
    kind: io.statefun.kafka.v1/egress
    spec:
      id: counter/kafka-results
      address: kafka.statefun-e2e:9092
      deliverySemantic:
        type: at-least-once
```

The egress id `counter/kafka-results` must match what the shared counter function sends to; results land on `counter.results` where the main suite filters by counterId, so leakage is harmless and no dedicated results consumer is needed for the fatal scenarios.

- [ ] **Step 2: Write `flink-deployment-invalid.yaml`**

Minimal on purpose: no HA, no checkpointing, no S3, no changelog, default (hashmap) state backend, default logging (plain text, easy to grep), 1 slot, small resources so the kind node (a ~7 GB GH runner) is not overloaded, `upgradeMode: stateless` because the test redeploys via delete+apply.

```yaml
# SPDX-License-Identifier: Apache-2.0


apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: statefun-e2e-invalid
  namespace: statefun-e2e
spec:
  image: flink-statefun:e2e
  imagePullPolicy: Never
  flinkVersion: "v2_2"
  serviceAccount: flink
  flinkConfiguration:
    classloader.parent-first-patterns.additional: "org.apache.flink.statefun;org.apache.kafka;com.google.protobuf"
    classloader.resolve-order: parent-first
    taskmanager.numberOfTaskSlots: "1"
    statefun.flink-job-name: statefun-k8s-e2e-invalid
    statefun.remote.module-name: "file:///opt/statefun/module.yaml"
    restart-strategy.type: fixed-delay
    restart-strategy.fixed-delay.attempts: "1"
    restart-strategy.fixed-delay.delay: "1s"
    pipeline.auto-generate-uids: "false"
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          volumeMounts:
            - name: module-config
              mountPath: /opt/statefun
      volumes:
        - name: module-config
          configMap:
            name: statefun-e2e-module-invalid
  jobManager:
    resource:
      memory: "1024m"
      cpu: 0.25
  taskManager:
    resource:
      memory: "1280m"
      cpu: 0.25
  job:
    jarURI: local:///opt/flink/statefun-flink-runner.jar
    parallelism: 1
    upgradeMode: stateless
```

- [ ] **Step 3: Commit**

```bash
git add statefun-e2e-tests/statefun-e2e-k8s-native/src/test/resources/k8s/module-configmap-invalid.yaml statefun-e2e-tests/statefun-e2e-k8s-native/src/test/resources/k8s/flink-deployment-invalid.yaml
git commit -m "e2e: manifests for isolated invalid-records FlinkDeployment"
```

---

### Task 2: Provision the invalid deployment in setup-cluster.sh

**Files:**
- Modify: `statefun-e2e-tests/statefun-e2e-k8s-native/scripts/setup-cluster.sh` (topics array line 21, module apply line 153, FlinkDeployment section lines 210-228)

**Interfaces:**
- Consumes: manifests from Task 1.
- Produces: topics `invalid.commands` / `invalid.results`; CR `statefun-e2e-invalid` READY after setup; a reusable `wait_for_flinkdeployment <name>` bash function.

- [ ] **Step 1: Add the new topics to the KAFKA_TOPICS array (line 21)**

```bash
KAFKA_TOPICS=(counter.commands counter.results counter.commands.ttl counter.results.ttl greeter.commands greeter.results invalid.commands invalid.results)
```

- [ ] **Step 2: Apply the invalid module ConfigMap next to the main one (after line 153)**

```bash
kubectl apply -f "${K8S_MANIFESTS}/module-configmap-invalid.yaml"
```

- [ ] **Step 3: Extract the READY wait into a function and wait for both CRs**

Replace the whole `--- FlinkDeployment ---` section (lines 210-228) with:

```bash
# --- FlinkDeployment --------------------------------------------------------

wait_for_flinkdeployment() {
  local name=$1
  echo "=== Waiting for FlinkDeployment ${name} to be READY ==="
  for i in $(seq 1 60); do
    status=$(kubectl get flinkdeployment "${name}" -n "${NAMESPACE}" \
      -o jsonpath='{.status.jobManagerDeploymentStatus}' 2>/dev/null || echo UNKNOWN)
    echo "  [${i}/60] FlinkDeployment ${name}: ${status}"
    [[ "${status}" == READY ]] && { echo "FlinkDeployment ${name} is READY!"; return 0; }
    if [[ $i -eq 60 ]]; then
      echo "ERROR: FlinkDeployment ${name} did not reach READY within 5 minutes"
      kubectl describe flinkdeployment "${name}" -n "${NAMESPACE}" || true
      kubectl logs -n "${NAMESPACE}" -l "app=${name},component=jobmanager" --tail=50 || true
      exit 1
    fi
    sleep 5
  done
}

echo "=== Deploying FlinkDeployments ==="
kubectl apply -f "${K8S_MANIFESTS}/flink-deployment.yaml"
kubectl apply -f "${K8S_MANIFESTS}/flink-deployment-invalid.yaml"
wait_for_flinkdeployment statefun-jobmanager
wait_for_flinkdeployment statefun-e2e-invalid
```

Note the original inline loop logged `kubectl logs -l component=jobmanager` without an app label; the function scopes it per deployment.

- [ ] **Step 4: Sanity-check the script parses**

Run: `bash -n statefun-e2e-tests/statefun-e2e-k8s-native/scripts/setup-cluster.sh`
Expected: exit 0, no output.

- [ ] **Step 5: Commit**

```bash
git add statefun-e2e-tests/statefun-e2e-k8s-native/scripts/setup-cluster.sh
git commit -m "e2e: provision isolated invalid-records deployment and topics"
```

---

### Task 3: Kubectl test util

**Files:**
- Create: `statefun-e2e-tests/statefun-e2e-k8s-native/src/test/java/org/apache/flink/statefun/e2e/k8s/util/Kubectl.java`

**Interfaces:**
- Consumes: `E2eContext.NAMESPACE`.
- Produces (all static, all throw RuntimeException on non-zero exit except where noted):
  - `String run(String... args)` — runs `kubectl <args>`, returns stdout.
  - `String jobState(String deployment)` — `.status.jobStatus.state` of a FlinkDeployment, empty string if unset.
  - `String jobManagerLog(String deployment)` — full JM pod log for `-l app=<deployment>,component=jobmanager`.
  - `void apply(String classpathResource)` — writes the resource to a temp file, `kubectl apply -f` it.
  - `void deleteFlinkDeployment(String name)` — `kubectl delete flinkdeployment <name> --ignore-not-found --wait=true`.
  - `void recreateTopic(String topic)` — exec into the kafka pod: delete topic (ignore failure if absent), then create it (1 partition, RF 1).

- [ ] **Step 1: Write the util**

```java
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.e2e.k8s.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Thin kubectl wrapper for E2E assertions, following the ProcessBuilder pattern of KubectlPortForward. */
public final class Kubectl {

  private Kubectl() {}

  /** Runs kubectl with the given args and returns stdout, failing the test on non-zero exit. */
  public static String run(String... args) {
    List<String> cmd = new ArrayList<>();
    cmd.add("kubectl");
    List.of(args).forEach(cmd::add);
    try {
      Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
      String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      if (!p.waitFor(120, TimeUnit.SECONDS) || p.exitValue() != 0) {
        throw new RuntimeException("kubectl " + String.join(" ", args) + " failed:\n" + out);
      }
      return out;
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("kubectl " + String.join(" ", args) + " failed", e);
    }
  }

  /** Current Flink job state of a FlinkDeployment CR, empty string while unset. */
  public static String jobState(String deployment) {
    return run("get", "flinkdeployment", deployment, "-n", E2eContext.NAMESPACE, "-o", "jsonpath={.status.jobStatus.state}").trim();
  }

  /** Full JobManager pod log of the given FlinkDeployment. */
  public static String jobManagerLog(String deployment) {
    return run("logs", "-n", E2eContext.NAMESPACE, "-l", "app=" + deployment + ",component=jobmanager", "--tail=-1");
  }

  /** Applies a classpath resource as a manifest via a temp file. */
  public static void apply(String classpathResource) {
    try (InputStream in = Kubectl.class.getClassLoader().getResourceAsStream(classpathResource)) {
      if (in == null) {
        throw new IllegalArgumentException("classpath resource not found: " + classpathResource);
      }
      Path tmp = Files.createTempFile("e2e-manifest-", ".yaml");
      Files.write(tmp, in.readAllBytes());
      run("apply", "-f", tmp.toString());
      Files.deleteIfExists(tmp);
    } catch (IOException e) {
      throw new RuntimeException("failed to apply " + classpathResource, e);
    }
  }

  /** Deletes a FlinkDeployment and waits for it to be gone; no-op when absent. */
  public static void deleteFlinkDeployment(String name) {
    run("delete", "flinkdeployment", name, "-n", E2eContext.NAMESPACE, "--ignore-not-found", "--wait=true");
  }

  /** Drops and recreates a topic on the in-cluster Kafka broker, discarding any poison records. */
  public static void recreateTopic(String topic) {
    String kafkaPod = run("get", "pod", "-n", E2eContext.NAMESPACE, "-l", "app=kafka", "-o", "jsonpath={.items[0].metadata.name}").trim();
    try {
      run("exec", "-n", E2eContext.NAMESPACE, kafkaPod, "--", "/opt/kafka/bin/kafka-topics.sh", "--delete", "--bootstrap-server", "localhost:9092", "--topic", topic);
    } catch (RuntimeException ignored) {
      // topic may not exist on the first reset; creation below is the part that matters
    }
    run("exec", "-n", E2eContext.NAMESPACE, kafkaPod, "--", "/opt/kafka/bin/kafka-topics.sh", "--create", "--if-not-exists", "--bootstrap-server", "localhost:9092", "--topic", topic, "--partitions", "1", "--replication-factor", "1");
  }
}
```

- [ ] **Step 2: Compile**

Run: `mvn -q -pl :statefun-e2e-k8s-native -am test-compile -DskipTests`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add statefun-e2e-tests/statefun-e2e-k8s-native/src/test/java/org/apache/flink/statefun/e2e/k8s/util/Kubectl.java
git commit -m "e2e: kubectl helper for CR state, JM logs, redeploy and topic reset"
```

---

### Task 4: StateFunKafkaInvalidRecordsE2E scenarios

**Files:**
- Create: `statefun-e2e-tests/statefun-e2e-k8s-native/src/test/java/org/apache/flink/statefun/e2e/k8s/StateFunKafkaInvalidRecordsE2E.java`

**Interfaces:**
- Consumes: `Kubectl.jobState`, `Kubectl.jobManagerLog`, `Kubectl.deleteFlinkDeployment`, `Kubectl.apply`, `Kubectl.recreateTopic` from Task 3; CR `statefun-e2e-invalid` and topic `invalid.commands` from Tasks 1-2; `KubectlPortForward`, `E2eContext` (existing).
- Produces: the extension point — each future strategy scenario is a new plain test method here.

- [ ] **Step 1: Write the test class**

```java
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.e2e.k8s;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.statefun.e2e.k8s.generated.E2EProtos.CounterCommand;
import org.apache.flink.statefun.e2e.k8s.util.E2eContext;
import org.apache.flink.statefun.e2e.k8s.util.Kubectl;
import org.apache.flink.statefun.e2e.k8s.util.KubectlPortForward;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Invalid-record scenarios against the dedicated statefun-e2e-invalid deployment. Each scenario is
 * one plain test method: produce a defect record, assert the observable outcome. Fatal scenarios
 * kill the job, so they end by resetting the topic and redeploying. When invalidRecordHandling
 * strategies (skip, forward) land, add non-fatal methods here and a ConfigMap variant per policy.
 */
@Tag("kafka")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StateFunKafkaInvalidRecordsE2E {

  private static final Logger LOG = LoggerFactory.getLogger(StateFunKafkaInvalidRecordsE2E.class);

  private static final String DEPLOYMENT = "statefun-e2e-invalid";
  private static final String COMMANDS_TOPIC = "invalid.commands";
  private static final int KAFKA_LOCAL_PORT = 9094;

  private KubectlPortForward kafkaForward;
  private KafkaProducer<byte[], byte[]> producer;

  @BeforeAll
  void setup() {
    kafkaForward = KubectlPortForward.fixed(E2eContext.NAMESPACE, "svc/kafka", KAFKA_LOCAL_PORT, KAFKA_LOCAL_PORT);
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + kafkaForward.localPort());
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producer = new KafkaProducer<>(p);
  }

  @Test
  @Order(1)
  void nullKeyRecordFailsJobWithRecordCoordinates() throws Exception {
    CounterCommand cmd = CounterCommand.newBuilder().setId("poison-null-key").setDelta(1).build();
    producer.send(new ProducerRecord<>(COMMANDS_TOPIC, null, cmd.toByteArray())).get(10, TimeUnit.SECONDS);
    producer.flush();
    LOG.info("Sent null-key poison record to {}", COMMANDS_TOPIC);

    awaitJobFailureWithDiagnostics("requires a UTF-8 key", "topic [" + COMMANDS_TOPIC + "]", "partition [0]", "offset [");
  }

  @Test
  @Order(2)
  void tombstoneRecordFailsJobWithRecordCoordinatesAndKey() throws Exception {
    resetTopicAndRedeploy();

    byte[] key = "poison-tombstone".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    producer.send(new ProducerRecord<>(COMMANDS_TOPIC, key, null)).get(10, TimeUnit.SECONDS);
    producer.flush();
    LOG.info("Sent tombstone poison record to {}", COMMANDS_TOPIC);

    awaitJobFailureWithDiagnostics("tombstone", "topic [" + COMMANDS_TOPIC + "]", "partition [0]", "offset [", "key [poison-tombstone]");
  }

  @AfterAll
  void teardown() {
    resetTopicAndRedeploy();
    if (producer != null) producer.close(java.time.Duration.ofSeconds(5));
    if (kafkaForward != null) kafkaForward.close();
  }

  /** Awaits terminal FAILED job state and every given fragment appearing in the JobManager log. */
  private static void awaitJobFailureWithDiagnostics(String... logFragments) {
    await().atMost(E2eContext.POLL_TIMEOUT).pollInterval(E2eContext.POLL_INTERVAL).untilAsserted(() -> {
      assertThat(Kubectl.jobState(DEPLOYMENT)).as("job state after poison record").isEqualTo("FAILED");
      String log = Kubectl.jobManagerLog(DEPLOYMENT);
      for (String fragment : logFragments) {
        assertThat(log).as("JobManager log should contain diagnostic fragment").contains(fragment);
      }
    });
  }

  /** Discards poison records and brings a fresh invalid-records job back to RUNNING. */
  private static void resetTopicAndRedeploy() {
    Kubectl.deleteFlinkDeployment(DEPLOYMENT);
    Kubectl.recreateTopic(COMMANDS_TOPIC);
    Kubectl.apply("k8s/flink-deployment-invalid.yaml");
    await().atMost(E2eContext.POLL_TIMEOUT).pollInterval(E2eContext.POLL_INTERVAL).untilAsserted(() -> assertThat(Kubectl.jobState(DEPLOYMENT)).as("redeployed job state").isEqualTo("RUNNING"));
  }
}
```

- [ ] **Step 2: Compile**

Run: `mvn -q -pl :statefun-e2e-k8s-native -am test-compile -DskipTests`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add statefun-e2e-tests/statefun-e2e-k8s-native/src/test/java/org/apache/flink/statefun/e2e/k8s/StateFunKafkaInvalidRecordsE2E.java
git commit -m "e2e: invalid-record scenarios - null key and tombstone fail with record coordinates"
```

---

### Task 5: Full Kafka E2E verification run

**Files:** none (verification only)

- [ ] **Step 1: Run the Kafka-tagged E2E suite (provisions kind, ~25-30 min, requires Docker)**

Run from repo root: `./mvnw verify -pl :statefun-e2e-k8s-native -am -DexcludedGroups=kinesis`
Expected: BUILD SUCCESS; failsafe summary shows `StateFunK8sE2E` (5 tests) and `StateFunKafkaInvalidRecordsE2E` (2 tests) all passing.

The new scenarios genuinely verify the stage-1 diagnostics: against pre-stage-1 code the null-key assertion would fail on the missing `topic [...]` fragment and the tombstone scenario would find an NPE instead of the `tombstone` diagnostic, so a green run is end-to-end evidence, not a tautology.

- [ ] **Step 2: If the run is green, update docs/architecture/e2e-tests.md coverage table**

Add a row to the table in the Coverage section:

```markdown
| `StateFunKafkaInvalidRecordsE2E` | Invalid-record diagnostics on the routable ingress: null key and tombstone fail the isolated job with record coordinates in the JM log |
```

- [ ] **Step 3: Commit**

```bash
git add docs/architecture/e2e-tests.md
git commit -m "docs: list invalid-record E2E scenarios in coverage table"
```
