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
import org.apache.flink.statefun.e2e.k8s.util.KubectlPortForward;
import org.apache.flink.statefun.e2e.k8s.util.LocalStackClients;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import software.amazon.awssdk.services.s3.S3Client;

/**
 * K8s E2E exercising Kafka ingress/egress + S3 checkpoint persistence. Expects the cluster to be
 * already provisioned by {@code scripts/setup-cluster.sh}.
 *
 * <p>Kafka is forwarded on fixed port 9094 (EXTERNAL advertised listener). LocalStack (S3) uses an
 * ephemeral port.
 */
@Tag("kafka")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StateFunK8sE2E {

  private static final Logger LOG = LoggerFactory.getLogger(StateFunK8sE2E.class);

  private static final int KAFKA_LOCAL_PORT = 9094;

  private static final String COUNTER_COMMANDS_TOPIC = "counter.commands";
  private static final String COUNTER_RESULTS_TOPIC = "counter.results";
  private static final String COUNTER_TTL_COMMANDS_TOPIC = "counter.commands.ttl";
  private static final String COUNTER_TTL_RESULTS_TOPIC = "counter.results.ttl";
  private static final String GREETER_COMMANDS_TOPIC = "greeter.commands";
  private static final String GREETER_RESULTS_TOPIC = "greeter.results";
  private static final String CHECKPOINTS_BUCKET = "statefun-checkpoints";

  private static final Duration STATE_TTL = Duration.ofSeconds(5);
  private static final Duration TTL_EXPIRY_PAD = Duration.ofSeconds(7);

  private KubectlPortForward kafkaForward;
  private KubectlPortForward localStackForward;
  private KafkaProducer<String, byte[]> producer;
  private KafkaConsumer<String, byte[]> counterResultsConsumer;
  private KafkaConsumer<String, byte[]> counterTtlResultsConsumer;
  private KafkaConsumer<String, byte[]> greeterResultsConsumer;
  private S3Client s3;

  @BeforeAll
  void setup() throws Exception {
    kafkaForward =
        KubectlPortForward.fixed(
            E2eContext.NAMESPACE, "svc/kafka", KAFKA_LOCAL_PORT, KAFKA_LOCAL_PORT);
    localStackForward = KubectlPortForward.ephemeral(E2eContext.NAMESPACE, "svc/localstack", 4566);
    LOG.info(
        "Kafka @127.0.0.1:{}, LocalStack @127.0.0.1:{}",
        kafkaForward.localPort(),
        localStackForward.localPort());

    String bootstrap = "127.0.0.1:" + kafkaForward.localPort();
    producer = createProducer(bootstrap);

    String runId = UUID.randomUUID().toString().substring(0, 8);
    counterResultsConsumer =
        createConsumer(bootstrap, "e2e-counter-" + runId, COUNTER_RESULTS_TOPIC);
    counterTtlResultsConsumer =
        createConsumer(bootstrap, "e2e-counter-ttl-" + runId, COUNTER_TTL_RESULTS_TOPIC);
    greeterResultsConsumer =
        createConsumer(bootstrap, "e2e-greeter-" + runId, GREETER_RESULTS_TOPIC);

    s3 = LocalStackClients.s3(localStackForward.localPort());
  }

  @Test
  @Order(1)
  void protobufCounterFunctionSumsDeltas() throws Exception {
    String counterId = "counter-" + UUID.randomUUID();
    int messages = 10;

    for (int i = 0; i < messages; i++) {
      CounterCommand cmd = CounterCommand.newBuilder().setId(counterId).setDelta(1).build();
      producer
          .send(new ProducerRecord<>(COUNTER_COMMANDS_TOPIC, counterId, cmd.toByteArray()))
          .get(10, TimeUnit.SECONDS);
    }
    producer.flush();
    LOG.info("Sent {} CounterCommand(s) for id={}", messages, counterId);

    await()
        .atMost(E2eContext.POLL_TIMEOUT)
        .pollInterval(E2eContext.POLL_INTERVAL)
        .untilAsserted(
            () -> {
              List<CounterResult> results =
                  StreamSupport.stream(
                          counterResultsConsumer.poll(Duration.ofSeconds(1)).spliterator(), false)
                      .map(ConsumerRecord::value)
                      .map(StateFunK8sE2E::parseCounterResult)
                      .filter(r -> counterId.equals(r.getId()))
                      .collect(Collectors.toList());
              long max = results.stream().mapToLong(CounterResult::getTotal).max().orElse(0);
              assertThat(max).as("counter should sum to %d", messages).isEqualTo(messages);
            });
  }

  @Test
  @Order(2)
  void jsonGreeterFunctionReturnsGreeting() throws Exception {
    String key = "alice-" + UUID.randomUUID().toString().substring(0, 8);
    String input = "{\"name\":\"Alice\"}";

    producer
        .send(
            new ProducerRecord<>(
                GREETER_COMMANDS_TOPIC, key, input.getBytes(StandardCharsets.UTF_8)))
        .get(10, TimeUnit.SECONDS);
    producer.flush();
    LOG.info("Sent greeter command for Alice with key={}", key);

    await()
        .atMost(E2eContext.POLL_TIMEOUT)
        .pollInterval(E2eContext.POLL_INTERVAL)
        .untilAsserted(
            () -> {
              List<String> greetings =
                  StreamSupport.stream(
                          greeterResultsConsumer.poll(Duration.ofSeconds(1)).spliterator(), false)
                      .map(r -> new String(r.value(), StandardCharsets.UTF_8))
                      .collect(Collectors.toList());
              assertThat(greetings).anyMatch(g -> g.contains("Hello, Alice!"));
            });
  }

  @Test
  @Order(3)
  void counterStateIsCleanedUpByTtl() throws Exception {
    String counterId = "counter-ttl-" + UUID.randomUUID();

    sendCounterCommand(COUNTER_TTL_COMMANDS_TOPIC, counterId, 5);
    awaitCounterTotal(counterTtlResultsConsumer, counterId, 5L);

    Thread.sleep(STATE_TTL.plus(TTL_EXPIRY_PAD).toMillis());

    sendCounterCommand(COUNTER_TTL_COMMANDS_TOPIC, counterId, 3);
    // total=8 would mean state was not wiped (prior 5 + new 3); assert explicitly to make a
    // TTL regression surface as "found forbidden [8]" rather than just "missing [3]".
    await()
        .atMost(E2eContext.POLL_TIMEOUT)
        .pollInterval(E2eContext.POLL_INTERVAL)
        .untilAsserted(
            () -> {
              List<CounterResult> results =
                  StreamSupport.stream(
                          counterTtlResultsConsumer.poll(Duration.ofSeconds(1)).spliterator(),
                          false)
                      .map(ConsumerRecord::value)
                      .map(StateFunK8sE2E::parseCounterResult)
                      .filter(r -> counterId.equals(r.getId()))
                      .collect(Collectors.toList());
              assertThat(results)
                  .as("counter result for id=%s after TTL expiry", counterId)
                  .extracting(CounterResult::getTotal)
                  .contains(3L)
                  .doesNotContain(8L);
            });
  }

  @Test
  @Order(4)
  void kafkaHeadersRoundTripThroughIngressAndEgress() throws Exception {
    String counterId = "counter-hdr-" + UUID.randomUUID();
    byte[] binaryHeaderValue = new byte[] {0x00, 0x01, (byte) 0xFF};

    CounterCommand cmd = CounterCommand.newBuilder().setId(counterId).setDelta(2).build();
    ProducerRecord<String, byte[]> record =
        new ProducerRecord<>(COUNTER_COMMANDS_TOPIC, counterId, cmd.toByteArray());
    record.headers().add("trace-id", "trace-abc".getBytes(StandardCharsets.UTF_8));
    record.headers().add("bin-header", binaryHeaderValue);
    record.headers().add("null-header", null);
    producer.send(record).get(10, TimeUnit.SECONDS);
    producer.flush();
    LOG.info("Sent CounterCommand with headers for id={}", counterId);

    await()
        .atMost(E2eContext.POLL_TIMEOUT)
        .pollInterval(E2eContext.POLL_INTERVAL)
        .untilAsserted(
            () -> {
              List<ConsumerRecord<String, byte[]>> results =
                  StreamSupport.stream(
                          counterResultsConsumer.poll(Duration.ofSeconds(1)).spliterator(), false)
                      .filter(r -> counterId.equals(parseCounterResult(r.value()).getId()))
                      .collect(Collectors.toList());
              assertThat(results).as("result record for id=%s", counterId).isNotEmpty();

              ConsumerRecord<String, byte[]> result = results.get(0);
              assertThat(headerValue(result, "trace-id"))
                  .as("ingress header echoed by the function")
                  .isEqualTo("trace-abc".getBytes(StandardCharsets.UTF_8));
              assertThat(headerValue(result, "bin-header"))
                  .as("binary ingress header echoed byte-exact")
                  .isEqualTo(binaryHeaderValue);
              assertThat(headerValue(result, "processed-by"))
                  .as("header set by the function itself")
                  .isEqualTo("counter-fn".getBytes(StandardCharsets.UTF_8));

              org.apache.kafka.common.header.Header nullEcho =
                  result.headers().lastHeader("null-header");
              assertThat(nullEcho).as("null-valued header is echoed, not dropped").isNotNull();
              assertThat(nullEcho.value()).as("null header value stays null, not empty").isNull();
            });
  }

  @Test
  @Order(5)
  void checkpointsWrittenToS3() {
    await()
        .atMost(E2eContext.POLL_TIMEOUT)
        .pollInterval(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              long count =
                  s3.listObjectsV2(r -> r.bucket(CHECKPOINTS_BUCKET).prefix("checkpoints/"))
                      .contents()
                      .size();
              assertThat(count).as("S3 bucket should contain checkpoint objects").isPositive();
            });
  }

  @AfterAll
  void teardown() {
    if (producer != null) producer.close(Duration.ofSeconds(5));
    if (counterResultsConsumer != null) counterResultsConsumer.close(Duration.ofSeconds(5));
    if (counterTtlResultsConsumer != null) counterTtlResultsConsumer.close(Duration.ofSeconds(5));
    if (greeterResultsConsumer != null) greeterResultsConsumer.close(Duration.ofSeconds(5));
    if (s3 != null) s3.close();
    if (kafkaForward != null) kafkaForward.close();
    if (localStackForward != null) localStackForward.close();
  }

  private void sendCounterCommand(String topic, String counterId, long delta) throws Exception {
    CounterCommand cmd = CounterCommand.newBuilder().setId(counterId).setDelta(delta).build();
    producer
        .send(new ProducerRecord<>(topic, counterId, cmd.toByteArray()))
        .get(10, TimeUnit.SECONDS);
    producer.flush();
    LOG.info("Sent CounterCommand(id={}, delta={}) to {}", counterId, delta, topic);
  }

  private static void awaitCounterTotal(
      KafkaConsumer<String, byte[]> consumer, String counterId, long expectedTotal) {
    await()
        .atMost(E2eContext.POLL_TIMEOUT)
        .pollInterval(E2eContext.POLL_INTERVAL)
        .untilAsserted(
            () -> {
              List<CounterResult> results =
                  StreamSupport.stream(consumer.poll(Duration.ofSeconds(1)).spliterator(), false)
                      .map(ConsumerRecord::value)
                      .map(StateFunK8sE2E::parseCounterResult)
                      .filter(r -> counterId.equals(r.getId()))
                      .collect(Collectors.toList());
              assertThat(results)
                  .as("counter result for id=%s", counterId)
                  .extracting(CounterResult::getTotal)
                  .contains(expectedTotal);
            });
  }

  private static byte[] headerValue(ConsumerRecord<String, byte[]> record, String key) {
    org.apache.kafka.common.header.Header header = record.headers().lastHeader(key);
    return header == null ? null : header.value();
  }

  private static CounterResult parseCounterResult(byte[] bytes) {
    try {
      return CounterResult.parseFrom(bytes);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse CounterResult", e);
    }
  }

  private static KafkaProducer<String, byte[]> createProducer(String bootstrapServers) {
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    return new KafkaProducer<>(p);
  }

  private static KafkaConsumer<String, byte[]> createConsumer(
      String bootstrapServers, String groupId, String topic) {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(p);
    consumer.subscribe(List.of(topic));
    return consumer;
  }
}
