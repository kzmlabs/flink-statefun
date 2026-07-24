// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Pins the header testing workflow: a function that reads {@link Message#headers()} can be driven
 * with {@link MessageBuilder} header overloads through {@link TestContext}, and its Kafka egress
 * headers asserted through {@link KafkaEgressCapture} — no protobuf hand-parsing in user tests.
 */
class KafkaHeaderTestingTest {

  private static final TypeName FN = TypeName.typeNameOf("io.test", "echo-fn");
  private static final TypeName EGRESS = TypeName.typeNameOf("io.test", "kafka-out");
  private static final Address SELF = new Address(FN, "id-1");

  /** Echoes all incoming headers onto its egress record and adds a retry counter. */
  private static final class HeaderEchoFn implements StatefulFunction {
    @Override
    public CompletableFuture<Void> apply(Context context, Message message) {
      KafkaEgressMessage.Builder egress =
          KafkaEgressMessage.forEgress(EGRESS)
              .withTopic("out")
              .withUtf8Key("k")
              .withUtf8Value(message.asUtf8String());
      message.headers().forEach(h -> egress.withHeader(h.key(), h.value()));
      egress.withHeader("retry-count", 10);
      context.send(egress.build());
      return context.done();
    }
  }

  @Test
  void headersFlowFromTestMessageThroughFunctionToEgressAssertions() throws Exception {
    Message message =
        MessageBuilder.forAddress(SELF)
            .withValue("payload")
            .withUtf8Header("trace-id", "abc-123")
            .withHeader("attempt", 3)
            .withHeader("null-header", (byte[]) null)
            .build();
    TestContext context = TestContext.forTarget(SELF);

    new HeaderEchoFn().apply(context, message).get();

    KafkaEgressCapture record =
        KafkaEgressCapture.of(context.getSentEgressMessages().get(0).message());
    assertThat(record.targetEgressId()).isEqualTo(EGRESS);
    assertThat(record.topic()).isEqualTo("out");
    assertThat(record.utf8Value()).isEqualTo("payload");
    assertThat(record.lastHeader("trace-id").orElseThrow().valueAsUtf8String())
        .isEqualTo("abc-123");
    assertThat(record.lastHeader("attempt").orElseThrow().valueAsInt()).isEqualTo(3);
    assertThat(record.lastHeader("null-header").orElseThrow().hasValue()).isFalse();
    assertThat(record.lastHeader("retry-count").orElseThrow().valueAsInt()).isEqualTo(10);
    assertThat(record.lastHeader("absent")).isEmpty();
  }

  @Test
  void fromMessagePreservesHeadersWhenForwarding() {
    Message original =
        MessageBuilder.forAddress(SELF)
            .withValue("payload")
            .withUtf8Header("trace-id", "abc-123")
            .withHeader("attempt", Types.integerType(), 3)
            .withHeader("null-header", (Slice) null)
            .build();

    Message forwarded =
        MessageBuilder.fromMessage(original)
            .withTargetAddress(TypeName.typeNameOf("io.test", "other-fn"), "id-2")
            .build();

    assertThat(forwarded.headers()).hasSize(3);
    assertThat(forwarded.headers().get(0).valueAsUtf8String()).isEqualTo("abc-123");
    assertThat(forwarded.headers().get(1).valueAs(Types.integerType())).isEqualTo(3);
    assertThat(forwarded.headers().get(2).hasValue()).isFalse();
    assertThat(forwarded.headers().get(2).value()).isNull();
  }

  @Test
  void messageBuilderSupportsAllPrimitiveHeaderTypes() {
    Message message =
        MessageBuilder.forAddress(SELF)
            .withValue("payload")
            .withHeader("int", 10)
            .withHeader("long", 42_000_000_000L)
            .withHeader("float", 0.25f)
            .withHeader("double", 0.5d)
            .withHeader("bool", true)
            .withUtf8Header("str", "hello")
            .withHeader("bytes", new byte[] {1, 2, 3})
            .build();

    assertThat(message.headers().get(0).valueAsInt()).isEqualTo(10);
    assertThat(message.headers().get(1).valueAsLong()).isEqualTo(42_000_000_000L);
    assertThat(message.headers().get(2).valueAsFloat()).isEqualTo(0.25f);
    assertThat(message.headers().get(3).valueAsDouble()).isEqualTo(0.5d);
    assertThat(message.headers().get(4).valueAsBoolean()).isTrue();
    assertThat(message.headers().get(5).valueAsUtf8String()).isEqualTo("hello");
    assertThat(message.headers().get(6).value().toByteArray()).containsExactly(1, 2, 3);
  }

  @Test
  void captureFailsLoudlyOnNonKafkaEgressMessage() {
    Message notAnEgress = MessageBuilder.forAddress(SELF).withValue("x").build();
    TestContext context = TestContext.forTarget(SELF);
    context.send(
        org.apache.flink.statefun.sdk.java.message.EgressMessageBuilder.forEgress(EGRESS)
            .withValue("plain, not a KafkaProducerRecord")
            .build());

    assertThatThrownBy(
            () -> KafkaEgressCapture.of(context.getSentEgressMessages().get(0).message()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(notAnEgress.headers()).isEmpty();
  }
}
