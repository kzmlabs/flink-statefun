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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.statefun.sdk.kinesis.ingress.IngressRecord;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;

class KinesisDeserializationSchemaDelegateTest {

  // A simple test double: extracts the UTF-8 string from the IngressRecord data and stores the
  // last-seen IngressRecord for assertion.
  private static final class CapturingDeserializer
      implements org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressDeserializer<String> {
    private static final long serialVersionUID = 1L;

    IngressRecord lastSeen;

    @Override
    public String deserialize(IngressRecord ingressRecord) {
      this.lastSeen = ingressRecord;
      return new String(ingressRecord.getData(), StandardCharsets.UTF_8);
    }
  }

  private static final class ListCollector<T> implements Collector<T> {
    final List<T> collected = new ArrayList<>();

    @Override
    public void collect(T record) {
      collected.add(record);
    }

    @Override
    public void close() {}
  }

  @Test
  void delegatesBytesToUserDeserializer() throws IOException {
    CapturingDeserializer userDeserializer = new CapturingDeserializer();
    KinesisDeserializationSchemaDelegate<String> delegate =
        new KinesisDeserializationSchemaDelegate<>(userDeserializer);

    Record kinesisRecord =
        Record.builder()
            .data(SdkBytes.fromByteArray("hello".getBytes(StandardCharsets.UTF_8)))
            .partitionKey("pk")
            .sequenceNumber("seq-1")
            .approximateArrivalTimestamp(Instant.ofEpochMilli(1000L))
            .build();

    ListCollector<String> collector = new ListCollector<>();
    delegate.deserialize(kinesisRecord, "my-stream", "shardId-000", collector);

    assertThat(collector.collected).containsExactly("hello");
  }

  @Test
  void preservesMetadata() throws IOException {
    CapturingDeserializer userDeserializer = new CapturingDeserializer();
    KinesisDeserializationSchemaDelegate<String> delegate =
        new KinesisDeserializationSchemaDelegate<>(userDeserializer);

    Instant arrivalTime = Instant.ofEpochMilli(9_876_543L);
    Record kinesisRecord =
        Record.builder()
            .data(SdkBytes.fromByteArray("payload".getBytes(StandardCharsets.UTF_8)))
            .partitionKey("pk1")
            .sequenceNumber("seq-99")
            .approximateArrivalTimestamp(arrivalTime)
            .build();

    ListCollector<String> collector = new ListCollector<>();
    delegate.deserialize(kinesisRecord, "events-stream", "shardId-001", collector);

    IngressRecord seen = userDeserializer.lastSeen;
    assertThat(seen.getPartitionKey()).isEqualTo("pk1");
    assertThat(seen.getSequenceNumber()).isEqualTo("seq-99");
    assertThat(seen.getApproximateArrivalTimestamp()).isEqualTo(arrivalTime.toEpochMilli());
    assertThat(seen.getStream()).isEqualTo("events-stream");
    assertThat(seen.getShardId()).isEqualTo("shardId-001");
  }
}
