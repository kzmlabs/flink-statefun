// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.e2e.k8s;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.statefun.e2e.k8s.generated.E2EProtos.CounterCommand;
import org.apache.flink.statefun.e2e.k8s.generated.E2EProtos.CounterResult;
import org.apache.flink.statefun.e2e.k8s.util.E2eContext;
import org.apache.flink.statefun.e2e.k8s.util.Kubectl;
import org.apache.flink.statefun.e2e.k8s.util.KubectlPortForward;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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
 * one plain test method: produce a defect record, assert the observable outcome. The deployment
 * starts under the default skip policy (no yaml); the fail scenarios swap the module ConfigMap to
 * the strict variant and redeploy. New strategies extend this class with a ConfigMap variant plus
 * new methods.
 */
@Tag("kafka")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StateFunKafkaInvalidRecordsE2E {

  private static final Logger LOG = LoggerFactory.getLogger(StateFunKafkaInvalidRecordsE2E.class);

  private static final String DEPLOYMENT = "statefun-e2e-invalid";
  private static final String COMMANDS_TOPIC = "invalid.commands";
  private static final String RESULTS_TOPIC = "counter.results";
  private static final int KAFKA_LOCAL_PORT = 9094;

  private KubectlPortForward kafkaForward;
  private KafkaProducer<byte[], byte[]> producer;
  private KafkaConsumer<byte[], byte[]> resultsConsumer;

  @BeforeAll
  void setup() throws Exception {
    kafkaForward = KubectlPortForward.fixed(E2eContext.NAMESPACE, "svc/kafka", KAFKA_LOCAL_PORT, KAFKA_LOCAL_PORT);
    String bootstrap = "127.0.0.1:" + kafkaForward.localPort();
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producer = new KafkaProducer<>(p);

    Properties c = new Properties();
    c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    c.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-invalid-" + UUID.randomUUID().toString().substring(0, 8));
    c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    resultsConsumer = new KafkaConsumer<>(c);
    resultsConsumer.subscribe(List.of(RESULTS_TOPIC));
  }

  @Test
  @Order(1)
  void skipPolicyDropsPoisonRecordsLogsThemAndKeepsProcessing() throws Exception {
    String counterId = "invalid-skip-" + UUID.randomUUID().toString().substring(0, 8);

    producer.send(new ProducerRecord<>(COMMANDS_TOPIC, null, CounterCommand.newBuilder().setId("poison").setDelta(1).build().toByteArray())).get(10, TimeUnit.SECONDS);
    producer.send(new ProducerRecord<>(COMMANDS_TOPIC, "poison-tombstone".getBytes(StandardCharsets.UTF_8), null)).get(10, TimeUnit.SECONDS);
    producer.send(new ProducerRecord<>(COMMANDS_TOPIC, counterId.getBytes(StandardCharsets.UTF_8), CounterCommand.newBuilder().setId(counterId).setDelta(3).build().toByteArray())).get(10, TimeUnit.SECONDS);
    producer.flush();
    LOG.info("Sent null-key poison, tombstone poison and a valid record (id={}) to {}", counterId, COMMANDS_TOPIC);

    await().atMost(E2eContext.POLL_TIMEOUT).pollInterval(E2eContext.POLL_INTERVAL).untilAsserted(() -> {
      List<CounterResult> results = StreamSupport.stream(resultsConsumer.poll(Duration.ofSeconds(1)).spliterator(), false)
          .map(ConsumerRecord::value)
          .map(StateFunKafkaInvalidRecordsE2E::parseCounterResult)
          .filter(r -> counterId.equals(r.getId()))
          .collect(Collectors.toList());
      assertThat(results).as("valid record after two skipped poisons").isNotEmpty();
    });

    assertThat(Kubectl.jobState(DEPLOYMENT)).as("job survives skipped poisons").isEqualTo("RUNNING");
    String tmLog = Kubectl.taskManagerLog(DEPLOYMENT);
    assertThat(tmLog).contains("Skipping invalid record");
    assertThat(tmLog).contains("defect [NULL_KEY]");
    assertThat(tmLog).contains("defect [NULL_VALUE]");
    assertThat(tmLog).contains("topic [" + COMMANDS_TOPIC + "]");
    assertThat(tmLog).contains("key [poison-tombstone]");
  }

  @Test
  @Order(2)
  void failPolicyNullKeyRecordFailsJobWithRecordCoordinates() throws Exception {
    swapToFailPolicyAndRedeploy();

    CounterCommand cmd = CounterCommand.newBuilder().setId("poison-null-key").setDelta(1).build();
    producer.send(new ProducerRecord<>(COMMANDS_TOPIC, null, cmd.toByteArray())).get(10, TimeUnit.SECONDS);
    producer.flush();
    LOG.info("Sent null-key poison record to {} under fail policy", COMMANDS_TOPIC);

    awaitJobFailureWithDiagnostics("requires a UTF-8 key", "topic [" + COMMANDS_TOPIC + "]", "partition [0]", "offset [");
  }

  @Test
  @Order(3)
  void failPolicyTombstoneRecordFailsJobWithRecordCoordinatesAndKey() throws Exception {
    resetTopicAndRedeploy();

    byte[] key = "poison-tombstone".getBytes(StandardCharsets.UTF_8);
    producer.send(new ProducerRecord<>(COMMANDS_TOPIC, key, null)).get(10, TimeUnit.SECONDS);
    producer.flush();
    LOG.info("Sent tombstone poison record to {} under fail policy", COMMANDS_TOPIC);

    awaitJobFailureWithDiagnostics("tombstone", "topic [" + COMMANDS_TOPIC + "]", "partition [0]", "offset [", "key [poison-tombstone]");
  }

  @AfterAll
  void teardown() {
    // restore the skip ConfigMap for local -Dskip.teardown reruns, but do not wait for RUNNING:
    // in CI the cluster is torn down right after, and a rerun's first scenario awaits its own outcome
    Kubectl.deleteFlinkDeployment(DEPLOYMENT);
    Kubectl.apply("k8s/module-configmap-invalid.yaml");
    Kubectl.recreateTopic(COMMANDS_TOPIC);
    Kubectl.apply("k8s/flink-deployment-invalid.yaml");
    if (producer != null) producer.close(Duration.ofSeconds(5));
    if (resultsConsumer != null) resultsConsumer.close(Duration.ofSeconds(5));
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

  /** Applies the strict-policy ConfigMap variant and brings a fresh job up on it. */
  private static void swapToFailPolicyAndRedeploy() {
    Kubectl.deleteFlinkDeployment(DEPLOYMENT);
    Kubectl.apply("k8s/module-configmap-invalid-fail.yaml");
    Kubectl.recreateTopic(COMMANDS_TOPIC);
    Kubectl.apply("k8s/flink-deployment-invalid.yaml");
    awaitRunning();
  }

  /** Discards poison records and brings a fresh job back to RUNNING under the current ConfigMap. */
  private static void resetTopicAndRedeploy() {
    Kubectl.deleteFlinkDeployment(DEPLOYMENT);
    Kubectl.recreateTopic(COMMANDS_TOPIC);
    Kubectl.apply("k8s/flink-deployment-invalid.yaml");
    awaitRunning();
  }

  private static void awaitRunning() {
    await().atMost(E2eContext.POLL_TIMEOUT).pollInterval(E2eContext.POLL_INTERVAL).untilAsserted(() -> assertThat(Kubectl.jobState(DEPLOYMENT)).as("redeployed job state").isEqualTo("RUNNING"));
  }

  private static CounterResult parseCounterResult(byte[] bytes) {
    try {
      return CounterResult.parseFrom(bytes);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse CounterResult", e);
    }
  }
}
