// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.junit.jupiter.api.Test;

class RoutableMessageBuilderTest {

  private static final FunctionType FN_A = new FunctionType("ns", "a");
  private static final FunctionType FN_B = new FunctionType("ns", "b");

  @Test
  void buildsMinimalRoutableMessageWithoutSource() {
    RoutableMessage m =
        RoutableMessageBuilder.builder()
            .withTargetAddress(FN_A, "id-1")
            .withMessageBody("payload")
            .build();

    assertThat(m.target()).isEqualTo(new Address(FN_A, "id-1"));
    assertThat(m.source()).isNull();
  }

  @Test
  void buildsRoutableMessageWithSourceAndTargetAddresses() {
    RoutableMessage m =
        RoutableMessageBuilder.builder()
            .withSourceAddress(FN_B, "src-1")
            .withTargetAddress(FN_A, "tgt-1")
            .withMessageBody("payload")
            .build();

    assertThat(m.source()).isEqualTo(new Address(FN_B, "src-1"));
    assertThat(m.target()).isEqualTo(new Address(FN_A, "tgt-1"));
  }

  @Test
  void withSourceAddressAcceptsExplicitNull() {
    RoutableMessage m =
        RoutableMessageBuilder.builder()
            .withSourceAddress((Address) null)
            .withTargetAddress(FN_A, "id")
            .withMessageBody("v")
            .build();

    assertThat(m.source()).isNull();
  }

  @Test
  void withTargetAddressRejectsNull() {
    RoutableMessageBuilder b = RoutableMessageBuilder.builder();
    assertThatThrownBy(() -> b.withTargetAddress(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void withMessageBodyRejectsNull() {
    RoutableMessageBuilder b = RoutableMessageBuilder.builder();
    assertThatThrownBy(() -> b.withMessageBody(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void cancellationTokenIsEmptyByDefaultOnFullMessageInterface() {
    Message m =
        (Message)
            RoutableMessageBuilder.builder()
                .withTargetAddress(FN_A, "id")
                .withMessageBody("v")
                .build();

    assertThat(m.cancellationToken()).isEmpty();
  }

  @Test
  void isBarrierMessageReturnsEmptyForRegularMessage() {
    Message m =
        (Message)
            RoutableMessageBuilder.builder()
                .withTargetAddress(FN_A, "id")
                .withMessageBody("v")
                .build();

    assertThat(m.isBarrierMessage().isEmpty()).isTrue();
  }
}
