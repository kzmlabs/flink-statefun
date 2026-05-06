// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.feedback;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class FeedbackChannelBrokerTest {

  // Each test gets a unique invocationId so re-runs in the same JVM don't interfere via the
  // singleton FeedbackChannelBroker.
  private static final AtomicLong INVOCATION_ID = new AtomicLong(System.nanoTime());

  @Test
  void getReturnsTheSameSingletonInstance() {
    assertThat(FeedbackChannelBroker.get()).isSameAs(FeedbackChannelBroker.get());
  }

  @Test
  void getChannelCreatesNewChannelOnFirstAccess() {
    SubtaskFeedbackKey<String> key = uniqueKey();

    FeedbackChannel<String> channel = FeedbackChannelBroker.get().getChannel(key);
    try {
      assertThat(channel).isNotNull();
    } finally {
      channel.close();
    }
  }

  @Test
  void getChannelReturnsSameChannelForSameKey() {
    SubtaskFeedbackKey<String> key = uniqueKey();

    FeedbackChannel<String> first = FeedbackChannelBroker.get().getChannel(key);
    try {
      FeedbackChannel<String> second = FeedbackChannelBroker.get().getChannel(key);

      assertThat(second).isSameAs(first);
    } finally {
      first.close();
    }
  }

  @Test
  void differentKeysReturnDifferentChannels() {
    SubtaskFeedbackKey<String> keyA = uniqueKey();
    SubtaskFeedbackKey<String> keyB = uniqueKey();

    FeedbackChannel<String> channelA = FeedbackChannelBroker.get().getChannel(keyA);
    FeedbackChannel<String> channelB = FeedbackChannelBroker.get().getChannel(keyB);
    try {
      assertThat(channelA).isNotSameAs(channelB);
    } finally {
      channelA.close();
      channelB.close();
    }
  }

  @Test
  void closingChannelRemovesItFromBrokerSoNextGetReturnsAFresh() {
    SubtaskFeedbackKey<String> key = uniqueKey();

    FeedbackChannel<String> first = FeedbackChannelBroker.get().getChannel(key);
    first.close();

    FeedbackChannel<String> second = FeedbackChannelBroker.get().getChannel(key);
    try {
      assertThat(second).isNotSameAs(first);
    } finally {
      second.close();
    }
  }

  @Test
  void getChannelRejectsNullKey() {
    assertThatThrownBy(() -> FeedbackChannelBroker.get().getChannel(null))
        .isInstanceOf(NullPointerException.class);
  }

  private static SubtaskFeedbackKey<String> uniqueKey() {
    return new FeedbackKey<String>("test-pipeline", INVOCATION_ID.incrementAndGet())
        .withSubTaskIndex(0, 0);
  }
}
