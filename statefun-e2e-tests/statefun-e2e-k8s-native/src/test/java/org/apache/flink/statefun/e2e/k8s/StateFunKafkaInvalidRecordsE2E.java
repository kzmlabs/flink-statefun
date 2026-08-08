// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.e2e.k8s;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
 * kill the job, so they are followed by a topic reset plus redeploy. When invalidRecordHandling
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
  void setup() throws Exception {
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

    byte[] key = "poison-tombstone".getBytes(StandardCharsets.UTF_8);
    producer.send(new ProducerRecord<>(COMMANDS_TOPIC, key, null)).get(10, TimeUnit.SECONDS);
    producer.flush();
    LOG.info("Sent tombstone poison record to {}", COMMANDS_TOPIC);

    awaitJobFailureWithDiagnostics("tombstone", "topic [" + COMMANDS_TOPIC + "]", "partition [0]", "offset [", "key [poison-tombstone]");
  }

  @AfterAll
  void teardown() {
    // reset for local -Dskip.teardown reruns, but do not wait for RUNNING: in CI the cluster is
    // torn down right after, and a rerun's first scenario awaits its own outcome anyway
    Kubectl.deleteFlinkDeployment(DEPLOYMENT);
    Kubectl.recreateTopic(COMMANDS_TOPIC);
    Kubectl.apply("k8s/flink-deployment-invalid.yaml");
    if (producer != null) producer.close(Duration.ofSeconds(5));
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
