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

import java.time.ZonedDateTime;
import java.util.List;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kinesis.ingress.IngressRecord;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressBuilder;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressDeserializer;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressStartupPosition;
import org.junit.jupiter.api.Test;

class KinesisSourceProviderTest {

  private static final IngressIdentifier<byte[]> TEST_ID =
      new IngressIdentifier<>(byte[].class, "test", "kinesis-ingress");

  private static final KinesisSourceProvider PROVIDER = new KinesisSourceProvider();

  // ---------------------------------------------------------------------------
  // Test 1: builds a KinesisStreamsSource from a spec with a stream ARN
  // ---------------------------------------------------------------------------

  @Test
  void buildsSourceFromSpecWithArn() {
    var spec =
        KinesisIngressBuilder.forIdentifier(TEST_ID)
            .withStreamArn("arn:aws:kinesis:us-east-1:000000000000:stream/events")
            .withStartupPosition(KinesisIngressStartupPosition.fromLatest())
            .withAwsRegion(
                org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion.ofCustomEndpoint(
                    "http://localhost:4566", "us-east-1"))
            .withAwsCredentials(
                org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials.basic(
                    "AKIAFAKE", "secret/FAKE"))
            .withDeserializer(TestDeserializer.class)
            .build();

    Source<byte[], ?, ?> result = PROVIDER.forSpec(spec);

    assertThat(result).isNotNull().isInstanceOf(KinesisStreamsSource.class);
  }

  // ---------------------------------------------------------------------------
  // Test 2: legacy single-stream name throws a helpful error
  // ---------------------------------------------------------------------------

  @Test
  void legacySingleStreamThrowsHelpfulError() {
    var spec =
        KinesisIngressBuilder.forIdentifier(TEST_ID)
            .withStream("my-stream-name")
            .withDeserializer(TestDeserializer.class)
            .build();

    assertThatThrownBy(() -> PROVIDER.forSpec(spec))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("withStreamArn");
  }

  // ---------------------------------------------------------------------------
  // Test 3: multi-stream throws a helpful error
  // ---------------------------------------------------------------------------

  @Test
  void multiStreamThrowsHelpfulError() {
    var spec =
        KinesisIngressBuilder.forIdentifier(TEST_ID)
            .withStreams(List.of("stream-a", "stream-b"))
            .withDeserializer(TestDeserializer.class)
            .build();

    assertThatThrownBy(() -> PROVIDER.forSpec(spec))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Multi-stream");
  }

  // ---------------------------------------------------------------------------
  // Test 4: wrong spec type throws IllegalArgumentException
  // ---------------------------------------------------------------------------

  @Test
  void wrongSpecTypeThrows() {
    IngressSpec<byte[]> wrongSpec =
        new IngressSpec<byte[]>() {
          @Override
          public IngressIdentifier<byte[]> id() {
            return TEST_ID;
          }

          @Override
          public IngressType type() {
            return new IngressType("wrong", "type");
          }
        };

    assertThatThrownBy(() -> PROVIDER.forSpec(wrongSpec))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------------------
  // Test 5: null spec throws NullPointerException
  // ---------------------------------------------------------------------------

  @Test
  void nullSpecThrows() {
    assertThatThrownBy(() -> PROVIDER.forSpec(null)).isInstanceOf(NullPointerException.class);
  }

  // ---------------------------------------------------------------------------
  // Test 6: LATEST startup position maps to InitialPosition.LATEST
  // ---------------------------------------------------------------------------

  @Test
  void startupPositionLatestMapsToLatest() {
    Configuration cfg = new Configuration();
    KinesisSourceProvider.applyStartupPosition(cfg, KinesisIngressStartupPosition.fromLatest());

    assertThat(cfg.get(KinesisSourceConfigOptions.STREAM_INITIAL_POSITION))
        .isEqualTo(KinesisSourceConfigOptions.InitialPosition.LATEST);
  }

  // ---------------------------------------------------------------------------
  // Test 7: EARLIEST startup position maps to InitialPosition.TRIM_HORIZON
  // ---------------------------------------------------------------------------

  @Test
  void startupPositionEarliestMapsToTrimHorizon() {
    Configuration cfg = new Configuration();
    KinesisSourceProvider.applyStartupPosition(cfg, KinesisIngressStartupPosition.fromEarliest());

    assertThat(cfg.get(KinesisSourceConfigOptions.STREAM_INITIAL_POSITION))
        .isEqualTo(KinesisSourceConfigOptions.InitialPosition.TRIM_HORIZON);
  }

  // ---------------------------------------------------------------------------
  // Test 8: DATE startup position maps to InitialPosition.AT_TIMESTAMP + timestamp string
  // ---------------------------------------------------------------------------

  @Test
  void startupPositionDateMapsToAtTimestamp() {
    ZonedDateTime dt = ZonedDateTime.parse("2024-06-15T12:30:45.000Z");
    Configuration cfg = new Configuration();
    KinesisSourceProvider.applyStartupPosition(cfg, KinesisIngressStartupPosition.fromDate(dt));

    assertThat(cfg.get(KinesisSourceConfigOptions.STREAM_INITIAL_POSITION))
        .isEqualTo(KinesisSourceConfigOptions.InitialPosition.AT_TIMESTAMP);
    assertThat(cfg.get(KinesisSourceConfigOptions.STREAM_INITIAL_TIMESTAMP))
        .isEqualTo("2024-06-15T12:30:45.000Z");
  }

  // ---------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------

  public static final class TestDeserializer implements KinesisIngressDeserializer<byte[]> {
    private static final long serialVersionUID = 1L;

    @Override
    public byte[] deserialize(IngressRecord ingressRecord) {
      return ingressRecord.getData();
    }
  }
}
