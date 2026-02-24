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
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.statefun.e2e.k8s.generated.E2EProtos.CounterCommand;
import org.apache.flink.statefun.e2e.k8s.generated.E2EProtos.CounterResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * K8s E2E test for StateFun. Expects the cluster and all infrastructure to be already deployed by
 * {@code scripts/setup-cluster.sh}. This test uses kubectl port-forward (via ProcessBuilder) for
 * Kafka and MinIO connectivity, then produces/consumes Kafka messages and asserts results.
 *
 * <p>Kafka must use fixed port 9094 to match the broker's EXTERNAL advertised listener. MinIO uses
 * an ephemeral port to avoid conflicts across repeated test runs.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StateFunK8sE2E {

  private static final Logger LOG = LoggerFactory.getLogger(StateFunK8sE2E.class);
  private static final String NAMESPACE = "statefun-e2e";
  private static final int KAFKA_LOCAL_PORT = 9094;
  private static final Pattern PORT_PATTERN =
      Pattern.compile("Forwarding from 127\\.0\\.0\\.1:(\\d+)");

  private final List<Process> portForwardProcesses = new ArrayList<>();
  private int minioLocalPort;
  private KafkaProducer<String, byte[]> producer;
  private KafkaConsumer<String, byte[]> protoConsumer;
  private KafkaConsumer<String, byte[]> jsonConsumer;

  @BeforeAll
  void setupKafkaClients() throws Exception {
    // Kafka needs fixed port 9094 (matching EXTERNAL advertised listener)
    startFixedPortForward("svc/kafka", KAFKA_LOCAL_PORT, KAFKA_LOCAL_PORT);
    waitForPort(KAFKA_LOCAL_PORT, 30);
    LOG.info("Kafka port-forwarded to 127.0.0.1:{}", KAFKA_LOCAL_PORT);

    // MinIO uses ephemeral port to avoid conflicts
    minioLocalPort = startEphemeralPortForward("svc/minio", 9000);
    LOG.info("MinIO port-forwarded to 127.0.0.1:{}", minioLocalPort);

    String bootstrapServers = "127.0.0.1:" + KAFKA_LOCAL_PORT;

    // Create producer
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producer = new KafkaProducer<>(producerProps);

    // Create consumers with unique group IDs per run to avoid stale offsets
    String runId = UUID.randomUUID().toString().substring(0, 8);
    protoConsumer = createConsumer(bootstrapServers, "e2e-proto-" + runId, "results-proto");
    jsonConsumer = createConsumer(bootstrapServers, "e2e-json-" + runId, "results-json");
  }

  @Test
  @Order(1)
  void protobufCounterFunction_sumsDeltasCorrectly() throws Exception {
    String counterId = "test-counter-" + UUID.randomUUID();
    int messageCount = 10;

    for (int i = 0; i < messageCount; i++) {
      CounterCommand cmd = CounterCommand.newBuilder().setId(counterId).setDelta(1).build();
      producer
          .send(new ProducerRecord<>("commands-proto", counterId, cmd.toByteArray()))
          .get(10, TimeUnit.SECONDS);
    }
    producer.flush();
    LOG.info("Sent {} CounterCommand messages for id={}", messageCount, counterId);

    List<CounterResult> results = new ArrayList<>();
    await()
        .atMost(Duration.ofMinutes(3))
        .pollInterval(Duration.ofSeconds(2))
        .untilAsserted(
            () -> {
              ConsumerRecords<String, byte[]> records = protoConsumer.poll(Duration.ofSeconds(1));
              for (ConsumerRecord<String, byte[]> record : records) {
                CounterResult result = CounterResult.parseFrom(record.value());
                if (counterId.equals(result.getId())) {
                  results.add(result);
                }
              }
              assertThat(results).as("Should receive counter results").isNotEmpty();
              long maxTotal = results.stream().mapToLong(CounterResult::getTotal).max().orElse(0);
              assertThat(maxTotal)
                  .as("Counter should sum to %d", messageCount)
                  .isEqualTo(messageCount);
            });

    LOG.info(
        "Received {} CounterResults, max total = {}",
        results.size(),
        results.stream().mapToLong(CounterResult::getTotal).max().orElse(0));
  }

  @Test
  @Order(2)
  void jsonGreeterFunction_returnsGreeting() throws Exception {
    String key = "alice-" + UUID.randomUUID().toString().substring(0, 8);
    String input = "{\"name\":\"Alice\"}";

    producer
        .send(new ProducerRecord<>("commands-json", key, input.getBytes(StandardCharsets.UTF_8)))
        .get(10, TimeUnit.SECONDS);
    producer.flush();
    LOG.info("Sent greeter command for Alice with key={}", key);

    List<String> greetings = new ArrayList<>();
    await()
        .atMost(Duration.ofMinutes(3))
        .pollInterval(Duration.ofSeconds(2))
        .untilAsserted(
            () -> {
              ConsumerRecords<String, byte[]> records = jsonConsumer.poll(Duration.ofSeconds(1));
              for (ConsumerRecord<String, byte[]> record : records) {
                greetings.add(new String(record.value(), StandardCharsets.UTF_8));
              }
              assertThat(greetings).as("Should receive at least one greeting").isNotEmpty();
              assertThat(greetings).anyMatch(g -> g.contains("Hello, Alice!"));
            });

    LOG.info("Received greeting: {}", greetings.get(greetings.size() - 1));
  }

  @Test
  @Order(3)
  void checkpointsWrittenToMinIO() throws Exception {
    MinioClient minioClient =
        MinioClient.builder()
            .endpoint("http://127.0.0.1:" + minioLocalPort)
            .credentials("minioadmin", "minioadmin")
            .build();

    await()
        .atMost(Duration.ofMinutes(3))
        .pollInterval(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              List<String> checkpointKeys = new ArrayList<>();
              Iterable<Result<Item>> objects =
                  minioClient.listObjects(
                      ListObjectsArgs.builder()
                          .bucket("statefun-e2e")
                          .prefix("checkpoints/")
                          .recursive(true)
                          .build());
              for (Result<Item> obj : objects) {
                checkpointKeys.add(obj.get().objectName());
              }
              LOG.info("Found {} checkpoint objects in MinIO", checkpointKeys.size());
              assertThat(checkpointKeys).as("MinIO should contain checkpoint files").isNotEmpty();
            });
  }

  @AfterAll
  void cleanup() {
    LOG.info("Cleaning up Kafka clients and port-forwards...");

    if (producer != null) {
      producer.close(Duration.ofSeconds(5));
    }
    if (protoConsumer != null) {
      protoConsumer.close(Duration.ofSeconds(5));
    }
    if (jsonConsumer != null) {
      jsonConsumer.close(Duration.ofSeconds(5));
    }
    for (Process p : portForwardProcesses) {
      p.destroyForcibly();
      try {
        p.waitFor(5, TimeUnit.SECONDS);
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Starts a port-forward with a fixed local port. Logs output to a file. */
  private void startFixedPortForward(String resource, int localPort, int remotePort)
      throws Exception {
    ProcessBuilder pb =
        new ProcessBuilder(
            "kubectl",
            "port-forward",
            "-n",
            NAMESPACE,
            "--address",
            "127.0.0.1",
            resource,
            localPort + ":" + remotePort);
    pb.redirectErrorStream(true);
    pb.redirectOutput(new File("target/port-forward-" + localPort + ".log"));
    Process process = pb.start();
    portForwardProcesses.add(process);
  }

  /** Starts a port-forward with an ephemeral local port and returns the assigned port. */
  private int startEphemeralPortForward(String resource, int remotePort) throws Exception {
    ProcessBuilder pb =
        new ProcessBuilder(
            "kubectl",
            "port-forward",
            "-n",
            NAMESPACE,
            "--address",
            "127.0.0.1",
            resource,
            "0:" + remotePort);
    pb.redirectErrorStream(true);
    Process process = pb.start();
    portForwardProcesses.add(process);

    // Read stdout to parse the assigned port
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      if (reader.ready()) {
        String line = reader.readLine();
        if (line != null) {
          Matcher m = PORT_PATTERN.matcher(line);
          if (m.find()) {
            int port = Integer.parseInt(m.group(1));
            // Drain remaining output in a daemon thread so process doesn't block
            Thread drainer = new Thread(() -> drainStream(reader), "pf-drain-" + port);
            drainer.setDaemon(true);
            drainer.start();
            waitForPort(port, 30);
            return port;
          }
        }
      }
      Thread.sleep(100);
    }
    throw new RuntimeException("Failed to parse port-forward output for " + resource);
  }

  private static void drainStream(BufferedReader reader) {
    try {
      while (reader.readLine() != null) {
        // discard
      }
    } catch (Exception ignored) {
      // stream closed
    }
  }

  private static void waitForPort(int port, int timeoutSeconds) throws Exception {
    long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
    while (System.currentTimeMillis() < deadline) {
      try (Socket s = new Socket(InetAddress.getByName("127.0.0.1"), port)) {
        return;
      } catch (Exception ignored) {
        Thread.sleep(500);
      }
    }
    throw new RuntimeException("Port " + port + " not available after " + timeoutSeconds + "s");
  }

  private static KafkaConsumer<String, byte[]> createConsumer(
      String bootstrapServers, String groupId, String topic) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(topic));
    return consumer;
  }
}
