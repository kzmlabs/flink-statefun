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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.flink.statefun.e2e.k8s.generated.E2EProtos.CounterCommand;
import org.apache.flink.statefun.e2e.k8s.generated.E2EProtos.CounterResult;
import org.apache.flink.statefun.e2e.k8s.util.E2eContext;
import org.apache.flink.statefun.e2e.k8s.util.KubectlPortForward;
import org.apache.flink.statefun.e2e.k8s.util.LocalStackClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

/**
 * K8s E2E exercising the Kinesis ingress/egress path end-to-end against a LocalStack-backed kind
 * cluster. Produces {@link CounterCommand} records to {@code counter.commands}, polls {@code
 * counter.results} via shard iterator, and asserts the counter function accumulates correctly.
 */
@Tag("kinesis")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StateFunKinesisE2E {

  private static final Logger LOG = LoggerFactory.getLogger(StateFunKinesisE2E.class);

  private static final String COMMANDS_STREAM = "counter.commands";
  private static final String RESULTS_STREAM = "counter.results";

  private KubectlPortForward localStackForward;
  private KinesisClient kinesis;

  @BeforeAll
  void setup() throws Exception {
    localStackForward = KubectlPortForward.ephemeral(E2eContext.NAMESPACE, "svc/localstack", 4566);
    LOG.info("LocalStack @127.0.0.1:{}", localStackForward.localPort());
    kinesis = LocalStackClients.kinesis(localStackForward.localPort());
  }

  @Test
  void protobufCounterFunctionSumsDeltas() {
    String counterId = "counter-" + UUID.randomUUID();
    int messages = 10;

    AtomicReference<String> shardIterator =
        new AtomicReference<>(shardIteratorFromTrimHorizon(RESULTS_STREAM));

    for (int i = 0; i < messages; i++) {
      CounterCommand cmd = CounterCommand.newBuilder().setId(counterId).setDelta(1).build();
      kinesis.putRecord(
          r ->
              r.streamName(COMMANDS_STREAM)
                  .partitionKey(counterId)
                  .data(SdkBytes.fromByteArray(cmd.toByteArray())));
    }
    LOG.info("Sent {} CounterCommand(s) for id={}", messages, counterId);

    await()
        .atMost(E2eContext.POLL_TIMEOUT)
        .pollInterval(E2eContext.POLL_INTERVAL)
        .untilAsserted(
            () -> {
              List<CounterResult> results = pollResults(shardIterator, counterId);
              long max = results.stream().mapToLong(CounterResult::getTotal).max().orElse(0);
              assertThat(max).as("counter should sum to %d", messages).isEqualTo(messages);
            });
  }

  @AfterAll
  void teardown() {
    if (kinesis != null) kinesis.close();
    if (localStackForward != null) localStackForward.close();
  }

  private String shardIteratorFromTrimHorizon(String streamName) {
    String shardId = kinesis.listShards(r -> r.streamName(streamName)).shards().get(0).shardId();
    return kinesis
        .getShardIterator(
            r ->
                r.streamName(streamName)
                    .shardId(shardId)
                    .shardIteratorType(ShardIteratorType.TRIM_HORIZON))
        .shardIterator();
  }

  private List<CounterResult> pollResults(AtomicReference<String> iterator, String counterId) {
    GetRecordsResponse response =
        kinesis.getRecords(r -> r.shardIterator(iterator.get()).limit(100));
    iterator.set(response.nextShardIterator());
    return response.records().stream()
        .map(StateFunKinesisE2E::parseCounterResult)
        .filter(cr -> counterId.equals(cr.getId()))
        .collect(Collectors.toList());
  }

  private static CounterResult parseCounterResult(Record record) {
    try {
      return CounterResult.parseFrom(record.data().asByteArray());
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse CounterResult", e);
    }
  }
}
