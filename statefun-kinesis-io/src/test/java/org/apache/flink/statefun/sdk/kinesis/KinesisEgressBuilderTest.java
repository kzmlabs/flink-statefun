// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
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
