// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.EgressMessageBuilder;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.apache.flink.statefun.sdk.java.storage.ConcurrentAddressScopedStorage;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConcurrentContextTest {

  private static final TypeName SELF_TYPE = TypeName.typeNameOf("ns", "self");
  private static final TypeName OTHER_TYPE = TypeName.typeNameOf("ns", "other");
  private static final TypeName EGRESS = TypeName.typeNameOf("io.test", "egress");

  private FromFunction.InvocationResponse.Builder responseBuilder;
  private ConcurrentContext context;

  @BeforeEach
  void setUp() {
    responseBuilder = FromFunction.InvocationResponse.newBuilder();
    context =
        new ConcurrentContext(
            new Address(SELF_TYPE, "id-1"),
            responseBuilder,
            new ConcurrentAddressScopedStorage(Collections.emptyList()));
  }

  @Test
  void selfReturnsTheConstructorAddress() {
    assertThat(context.self()).isEqualTo(new Address(SELF_TYPE, "id-1"));
  }

  @Test
  void callerIsEmptyByDefault() {
    assertThat(context.caller()).isEmpty();
  }

  @Test
  void setCallerSurfacesViaCallerOptional() {
    Address callerAddr = new Address(OTHER_TYPE, "caller-id");

    context.setCaller(callerAddr);

    assertThat(context.caller()).isEqualTo(Optional.of(callerAddr));
  }

  @Test
  void sendAppendsOutgoingMessageToResponseBuilder() {
    context.send(MessageBuilder.forAddress(OTHER_TYPE, "tgt").withValue(42L).build());

    FromFunction.InvocationResponse response = context.finalBuilder().build();
    assertThat(response.getOutgoingMessagesCount()).isEqualTo(1);
    FromFunction.Invocation invocation = response.getOutgoingMessages(0);
    assertThat(invocation.getTarget().getNamespace()).isEqualTo("ns");
    assertThat(invocation.getTarget().getType()).isEqualTo("other");
    assertThat(invocation.getTarget().getId()).isEqualTo("tgt");
  }

  @Test
  void sendAfterAppendsDelayedInvocation() {
    context.sendAfter(
        Duration.ofMinutes(5),
        MessageBuilder.forAddress(OTHER_TYPE, "tgt").withValue(1L).build());

    FromFunction.InvocationResponse response = context.finalBuilder().build();
    assertThat(response.getDelayedInvocationsCount()).isEqualTo(1);
    FromFunction.DelayedInvocation delayed = response.getDelayedInvocations(0);
    assertThat(delayed.getDelayInMs()).isEqualTo(Duration.ofMinutes(5).toMillis());
    assertThat(delayed.getCancellationToken()).isEmpty();
    assertThat(delayed.getIsCancellationRequest()).isFalse();
  }

  @Test
  void sendAfterWithCancellationTokenStoresToken() {
    context.sendAfter(
        Duration.ofSeconds(30),
        "tok-123",
        MessageBuilder.forAddress(OTHER_TYPE, "tgt").withValue(1L).build());

    FromFunction.DelayedInvocation delayed = context.finalBuilder().build().getDelayedInvocations(0);
    assertThat(delayed.getCancellationToken()).isEqualTo("tok-123");
    assertThat(delayed.getIsCancellationRequest()).isFalse();
  }

  @Test
  void sendAfterWithEmptyCancellationTokenIsRejected() {
    assertThatThrownBy(
            () ->
                context.sendAfter(
                    Duration.ofSeconds(1),
                    "",
                    MessageBuilder.forAddress(OTHER_TYPE, "tgt").withValue(1L).build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cancellation token");
  }

  @Test
  void sendAfterWithNullCancellationTokenIsRejected() {
    assertThatThrownBy(
            () ->
                context.sendAfter(
                    Duration.ofSeconds(1),
                    null,
                    MessageBuilder.forAddress(OTHER_TYPE, "tgt").withValue(1L).build()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void cancelDelayedMessageEmitsCancellationRecord() {
    context.cancelDelayedMessage("tok-123");

    FromFunction.DelayedInvocation cancellation =
        context.finalBuilder().build().getDelayedInvocations(0);
    assertThat(cancellation.getIsCancellationRequest()).isTrue();
    assertThat(cancellation.getCancellationToken()).isEqualTo("tok-123");
  }

  @Test
  void cancelDelayedMessageWithEmptyTokenIsRejected() {
    assertThatThrownBy(() -> context.cancelDelayedMessage(""))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> context.cancelDelayedMessage(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void egressSendAppendsToOutgoingEgresses() {
    context.send(EgressMessageBuilder.forEgress(EGRESS).withValue("payload").build());

    FromFunction.InvocationResponse response = context.finalBuilder().build();
    assertThat(response.getOutgoingEgressesCount()).isEqualTo(1);
    FromFunction.EgressMessage egressMsg = response.getOutgoingEgresses(0);
    assertThat(egressMsg.getEgressNamespace()).isEqualTo("io.test");
    assertThat(egressMsg.getEgressType()).isEqualTo("egress");
  }

  @Test
  void afterFinalBuilderFurtherSendsAreRejected() {
    context.finalBuilder();

    // Once finalBuilder() is called the context is closed; pin that any further mutation
    // throws (otherwise late writes after the response is built would silently no-op).
    assertThatThrownBy(
            () ->
                context.send(
                    MessageBuilder.forAddress(OTHER_TYPE, "tgt").withValue(1L).build()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("already completed");
    assertThatThrownBy(
            () ->
                context.sendAfter(
                    Duration.ofSeconds(1),
                    MessageBuilder.forAddress(OTHER_TYPE, "tgt").withValue(1L).build()))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> context.cancelDelayedMessage("tok"))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(
            () -> context.send(EgressMessageBuilder.forEgress(EGRESS).withValue("v").build()))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void sendRejectsNullMessage() {
    assertThatThrownBy(() -> context.send((org.apache.flink.statefun.sdk.java.message.Message) null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(
            () ->
                context.send((org.apache.flink.statefun.sdk.java.message.EgressMessage) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void storageReturnsTheConstructorStorage() {
    assertThat(context.storage()).isNotNull();
  }
}
