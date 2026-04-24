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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kinesis.sink.PartitionKeyGenerator;
import org.apache.flink.statefun.sdk.kinesis.egress.EgressRecord;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSerializer;
import org.junit.jupiter.api.Test;

class KinesisSerializationSchemaDelegateTest {

  // A simple test double: produces an EgressRecord with the given bytes as data and a fixed
  // partition key derived from the input string.
  private static final class FixedEgressSerializer implements KinesisEgressSerializer<String> {
    private static final long serialVersionUID = 1L;

    @Override
    public EgressRecord serialize(String value) {
      return EgressRecord.newBuilder()
          .withData(value.getBytes(StandardCharsets.UTF_8))
          .withPartitionKey("pk-" + value)
          .withStream("my-stream")
          .build();
    }
  }

  @Test
  void serializeReturnsEgressRecordData() {
    KinesisSerializationSchemaDelegate<String> delegate =
        new KinesisSerializationSchemaDelegate<>(new FixedEgressSerializer());

    byte[] result = delegate.serialize("hello");

    assertThat(result).isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void partitionKeyGeneratorReturnsEgressRecordKey() {
    KinesisSerializationSchemaDelegate<String> delegate =
        new KinesisSerializationSchemaDelegate<>(new FixedEgressSerializer());
    PartitionKeyGenerator<String> keyGen = delegate.partitionKeyGenerator();

    String key = keyGen.apply("hello");

    assertThat(key).isEqualTo("pk-hello");
  }

  @Test
  void handlesDifferentElementsIndependently() {
    KinesisSerializationSchemaDelegate<String> delegate =
        new KinesisSerializationSchemaDelegate<>(new FixedEgressSerializer());
    PartitionKeyGenerator<String> keyGen = delegate.partitionKeyGenerator();

    assertThat(keyGen.apply("alpha")).isEqualTo("pk-alpha");
    assertThat(keyGen.apply("beta")).isEqualTo("pk-beta");
  }

  @Test
  void isSerializable() {
    KinesisSerializationSchemaDelegate<String> delegate =
        new KinesisSerializationSchemaDelegate<>(new FixedEgressSerializer());
    PartitionKeyGenerator<String> keyGen = delegate.partitionKeyGenerator();

    assertThat(delegate).isInstanceOf(Serializable.class);
    assertThat(keyGen).isInstanceOf(Serializable.class);
  }
}
