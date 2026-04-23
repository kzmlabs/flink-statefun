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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;
import org.apache.flink.statefun.sdk.kinesis.egress.EgressRecord;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressBuilder;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSerializer;
import org.junit.jupiter.api.Test;

class KinesisSinkProviderTest {

  private static final EgressIdentifier<byte[]> TEST_ID =
      new EgressIdentifier<>("test", "kinesis-egress", byte[].class);

  private static final KinesisSinkProvider PROVIDER = new KinesisSinkProvider();

  // ---------------------------------------------------------------------------
  // Test 1: builds a KinesisStreamsSink from a valid spec
  // ---------------------------------------------------------------------------

  @Test
  void buildsSinkFromValidSpec() {
    var spec =
        KinesisEgressBuilder.forIdentifier(TEST_ID)
            .withStreamName("my-output-stream")
            .withAwsRegion(AwsRegion.ofId("us-east-1"))
            .withAwsCredentials(AwsCredentials.basic("AKIAFAKE", "secret/FAKE"))
            .withSerializer(TestEgressSerializer.class)
            .build();

    Sink<byte[]> result = PROVIDER.forSpec(spec);

    assertThat(result).isNotNull().isInstanceOf(KinesisStreamsSink.class);
  }

  // ---------------------------------------------------------------------------
  // Test 2: wrong spec type throws IllegalArgumentException
  // ---------------------------------------------------------------------------

  @Test
  void wrongSpecTypeThrows() {
    EgressSpec<byte[]> wrongSpec =
        new EgressSpec<byte[]>() {
          @Override
          public EgressIdentifier<byte[]> id() {
            return TEST_ID;
          }

          @Override
          public EgressType type() {
            return new EgressType("wrong", "type");
          }
        };

    assertThatThrownBy(() -> PROVIDER.forSpec(wrongSpec))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------------------
  // Test 3: null spec throws NullPointerException
  // ---------------------------------------------------------------------------

  @Test
  void nullSpecThrows() {
    assertThatThrownBy(() -> PROVIDER.forSpec(null)).isInstanceOf(NullPointerException.class);
  }

  // ---------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------

  public static class TestEgressSerializer implements KinesisEgressSerializer<byte[]> {

    @Override
    public EgressRecord serialize(byte[] value) {
      return EgressRecord.newBuilder()
          .withData(value)
          .withStream("ignored-by-runtime")
          .withPartitionKey("test-key")
          .build();
    }
  }
}
