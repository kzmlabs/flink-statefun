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

package org.apache.flink.statefun.e2e.k8s;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.statefun.e2e.k8s.generated.E2EProtos.CounterCommand;
import org.apache.flink.statefun.e2e.k8s.generated.E2EProtos.CounterResult;
import org.apache.flink.statefun.e2e.k8s.util.KubectlPortForward;
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

/**
 * K8s E2E for the Kafka ingress/egress path. Expects the cluster and all infrastructure to be
 * already deployed by {@code scripts/setup-cluster.sh}. Uses {@link KubectlPortForward} for Kafka
 * and MinIO access.
 *
 * <p>Kafka is forwarded on fixed port 9094 to match the broker's EXTERNAL advertised listener.
 * MinIO uses an ephemeral local port to avoid conflicts across repeated test runs.
 */
@Tag("kafka")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StateFunK8sE2E {

  private static final Logger LOG = LoggerFactory.getLogger(StateFunK8sE2E.class);
  private static final String NAMESPACE = "statefun-e2e";
  private static final int KAFKA_LOCAL_PORT = 9094;
  private static final Duration POLL_TIMEOUT = Duration.ofMinutes(3);
  private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);

  private KubectlPortForward kafkaForward;
  private KubectlPortForward minioForward;
  private KafkaProducer<String, byte[]> producer;
  private KafkaConsumer<String, byte[]> protoConsumer;
  private KafkaConsumer<String, byte[]> jsonConsumer;

  @BeforeAll
  void setup() throws Exception {
    kafkaForward =
        KubectlPortForward.fixed(NAMESPACE, "svc/kafka", KAFKA_LOCAL_PORT, KAFKA_LOCAL_PORT);
    minioForward = KubectlPortForward.ephemeral(NAMESPACE, "svc/minio", 9000);
    LOG.info(
        "Kafka @127.0.0.1:{}, MinIO @127.0.0.1:{}",
        kafkaForward.localPort(),
        minioForward.localPort());

    String bootstrap = "127.0.0.1:" + kafkaForward.localPort();
    producer = createProducer(bootstrap);

    String runId = UUID.randomUUID().toString().substring(0, 8);
    protoConsumer = createConsumer(bootstrap, "e2e-proto-" + runId, "results-proto");
    jsonConsumer = createConsumer(bootstrap, "e2e-json-" + runId, "results-json");
  }

  @Test
  @Order(1)
  void protobufCounterFunctionSumsDeltas() throws Exception {
    String counterId = "counter-" + UUID.randomUUID();
    int messages = 10;

    for (int i = 0; i < messages; i++) {
      CounterCommand cmd = CounterCommand.newBuilder().setId(counterId).setDelta(1).build();
      producer
          .send(new ProducerRecord<>("commands-proto", counterId, cmd.toByteArray()))
          .get(10, java.util.concurrent.TimeUnit.SECONDS);
    }
    producer.flush();
    LOG.info("Sent {} CounterCommand(s) for id={}", messages, counterId);

    await()
        .atMost(POLL_TIMEOUT)
        .pollInterval(POLL_INTERVAL)
        .untilAsserted(
            () -> {
              List<CounterResult> results =
                  StreamSupport.stream(
                          protoConsumer.poll(Duration.ofSeconds(1)).spliterator(), false)
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
        .send(new ProducerRecord<>("commands-json", key, input.getBytes(StandardCharsets.UTF_8)))
        .get(10, java.util.concurrent.TimeUnit.SECONDS);
    producer.flush();
    LOG.info("Sent greeter command for Alice with key={}", key);

    await()
        .atMost(POLL_TIMEOUT)
        .pollInterval(POLL_INTERVAL)
        .untilAsserted(
            () -> {
              List<String> greetings =
                  StreamSupport.stream(
                          jsonConsumer.poll(Duration.ofSeconds(1)).spliterator(), false)
                      .map(r -> new String(r.value(), StandardCharsets.UTF_8))
                      .collect(Collectors.toList());
              assertThat(greetings).anyMatch(g -> g.contains("Hello, Alice!"));
            });
  }

  @Test
  @Order(3)
  void checkpointsWrittenToMinIO() {
    MinioClient minio =
        MinioClient.builder()
            .endpoint("http://127.0.0.1:" + minioForward.localPort())
            .credentials("minioadmin", "minioadmin")
            .build();

    await()
        .atMost(POLL_TIMEOUT)
        .pollInterval(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              Iterable<Result<Item>> objects =
                  minio.listObjects(
                      ListObjectsArgs.builder()
                          .bucket("statefun-e2e")
                          .prefix("checkpoints/")
                          .recursive(true)
                          .build());
              long count =
                  StreamSupport.stream(objects.spliterator(), false)
                      .peek(r -> logIfError(r))
                      .count();
              assertThat(count).as("MinIO should contain checkpoint objects").isPositive();
            });
  }

  @AfterAll
  void teardown() {
    if (producer != null) producer.close(Duration.ofSeconds(5));
    if (protoConsumer != null) protoConsumer.close(Duration.ofSeconds(5));
    if (jsonConsumer != null) jsonConsumer.close(Duration.ofSeconds(5));
    if (kafkaForward != null) kafkaForward.close();
    if (minioForward != null) minioForward.close();
  }

  // --- helpers ---

  private static CounterResult parseCounterResult(byte[] bytes) {
    try {
      return CounterResult.parseFrom(bytes);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse CounterResult", e);
    }
  }

  private static void logIfError(Result<Item> result) {
    try {
      result.get();
    } catch (Exception e) {
      LOG.warn("MinIO list entry error: {}", e.getMessage());
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
