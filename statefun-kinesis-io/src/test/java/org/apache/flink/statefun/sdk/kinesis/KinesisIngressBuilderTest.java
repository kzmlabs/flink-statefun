// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.kinesis;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.kinesis.ingress.IngressRecord;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressBuilder;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressDeserializer;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressSpec;
import org.junit.jupiter.api.Test;

public class KinesisIngressBuilderTest {

  private static final IngressIdentifier<String> ID =
      new IngressIdentifier<>(String.class, "namespace", "name");

  private static final String STREAM_NAME = "test-stream";

  private static final String STREAM_ARN = "arn:aws:kinesis:us-east-1:000000000000:stream/events";

  @Test
  public void exampleUsage() {
    final KinesisIngressSpec<String> kinesisIngressSpec =
        KinesisIngressBuilder.forIdentifier(ID)
            .withDeserializer(TestDeserializer.class)
            .withStream(STREAM_NAME)
            .build();

    assertThat(kinesisIngressSpec.id(), is(ID));
    assertThat(kinesisIngressSpec.streams(), is(Collections.singletonList(STREAM_NAME)));
    assertNull(kinesisIngressSpec.streamArn());
    assertTrue(kinesisIngressSpec.awsRegion().get().isDefault());
    assertTrue(kinesisIngressSpec.awsCredentials().get().isDefault());
    assertThat(kinesisIngressSpec.deserializer(), instanceOf(TestDeserializer.class));
    assertTrue(kinesisIngressSpec.startupPosition().isLatest());
    assertTrue(kinesisIngressSpec.properties().isEmpty());
  }

  @Test
  public void streamArn_set_build_ok() {
    final KinesisIngressSpec<String> spec =
        KinesisIngressBuilder.forIdentifier(ID)
            .withDeserializer(TestDeserializer.class)
            .withStreamArn(STREAM_ARN)
            .build();

    assertEquals(STREAM_ARN, spec.streamArn());
    assertTrue(spec.streams().isEmpty());
  }

  @Test
  public void bothStreamsAndArn_throws() {
    assertThrows(
        IllegalStateException.class,
        () ->
            KinesisIngressBuilder.forIdentifier(ID)
                .withDeserializer(TestDeserializer.class)
                .withStream(STREAM_NAME)
                .withStreamArn(STREAM_ARN)
                .build());
  }

  @Test
  public void neitherStreamsNorArn_throws() {
    assertThrows(
        IllegalStateException.class,
        () ->
            KinesisIngressBuilder.forIdentifier(ID)
                .withDeserializer(TestDeserializer.class)
                .build());
  }

  private static final class TestDeserializer implements KinesisIngressDeserializer<String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String deserialize(IngressRecord ingressRecord) {
      return null;
    }
  }
}
