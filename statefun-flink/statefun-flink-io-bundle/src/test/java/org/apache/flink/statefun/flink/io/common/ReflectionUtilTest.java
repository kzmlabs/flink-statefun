// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.common;

import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

public class ReflectionUtilTest {

  private static final class Serializer implements KafkaIngressDeserializer<String> {

    private static final long serialVersionUID = 1;

    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> input) {
      return null;
    }
  }

  @Test
  public void example() {
    Serializer serializer = ReflectionUtil.instantiate(Serializer.class);

    assertThat(serializer, CoreMatchers.notNullValue());
  }
}
