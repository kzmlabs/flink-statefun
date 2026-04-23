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
package org.apache.flink.statefun.sdk.kinesis;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.kinesis.egress.EgressRecord;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressBuilder;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSerializer;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSpec;
import org.junit.jupiter.api.Test;

public class KinesisEgressBuilderTest {

  private static final EgressIdentifier<String> ID =
      new EgressIdentifier<>("namespace", "name", String.class);

  private static final String STREAM_NAME = "my-output-stream";

  @Test
  public void exampleUsage() {
    final KinesisEgressSpec<String> kinesisEgressSpec =
        KinesisEgressBuilder.forIdentifier(ID)
            .withSerializer(TestSerializer.class)
            .withStreamName(STREAM_NAME)
            .build();

    assertThat(kinesisEgressSpec.id(), is(ID));
    assertTrue(kinesisEgressSpec.awsRegion().isDefault());
    assertTrue(kinesisEgressSpec.awsCredentials().isDefault());
    assertEquals(TestSerializer.class, kinesisEgressSpec.serializerClass());
    assertTrue(kinesisEgressSpec.clientConfigurationProperties().isEmpty());
  }

  @Test
  public void streamName_set_build_ok() {
    final KinesisEgressSpec<String> spec =
        KinesisEgressBuilder.forIdentifier(ID)
            .withSerializer(TestSerializer.class)
            .withStreamName(STREAM_NAME)
            .build();

    assertEquals(STREAM_NAME, spec.streamName());
  }

  @Test
  public void streamName_missing_throws() {
    assertThrows(
        IllegalStateException.class,
        () -> KinesisEgressBuilder.forIdentifier(ID).withSerializer(TestSerializer.class).build());
  }

  private static final class TestSerializer implements KinesisEgressSerializer<String> {

    private static final long serialVersionUID = 1L;

    @Override
    public EgressRecord serialize(String value) {
      return null;
    }
  }
}
