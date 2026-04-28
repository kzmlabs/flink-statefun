// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
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
