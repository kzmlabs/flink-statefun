// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.feedback;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class FeedbackKeyTest {

  @Test
  void asColocationKeyContainsPipelineNameAndInvocationId() {
    FeedbackKey<String> key = new FeedbackKey<>("my-pipeline", 42L);

    assertThat(key.asColocationKey()).isEqualTo("CO-LOCATION/my-pipeline/42");
  }

  @Test
  void equalKeysMatchOnPipelineNameAndInvocationId() {
    FeedbackKey<String> a = new FeedbackKey<>("p", 1L);
    FeedbackKey<String> b = new FeedbackKey<>("p", 1L);

    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  @Test
  void differentInvocationIdMakesKeysUnequal() {
    FeedbackKey<String> a = new FeedbackKey<>("p", 1L);
    FeedbackKey<String> b = new FeedbackKey<>("p", 2L);

    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void differentPipelineMakesKeysUnequal() {
    FeedbackKey<String> a = new FeedbackKey<>("p1", 1L);
    FeedbackKey<String> b = new FeedbackKey<>("p2", 1L);

    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void equalsToSelfAndNotEqualsToNullOrOther() {
    FeedbackKey<String> a = new FeedbackKey<>("p", 1L);

    assertThat(a).isEqualTo(a).isNotEqualTo(null).isNotEqualTo("string");
  }

  @Test
  void constructorRejectsNullPipelineName() {
    assertThatThrownBy(() -> new FeedbackKey<>(null, 1L)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void withSubTaskIndexProducesCorrespondingSubtaskKey() {
    FeedbackKey<String> base = new FeedbackKey<>("p", 7L);

    SubtaskFeedbackKey<String> a = base.withSubTaskIndex(3, 0);
    SubtaskFeedbackKey<String> b = base.withSubTaskIndex(3, 0);

    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  @Test
  void subtaskKeysDifferByIndex() {
    FeedbackKey<String> base = new FeedbackKey<>("p", 7L);

    SubtaskFeedbackKey<String> a = base.withSubTaskIndex(0, 0);
    SubtaskFeedbackKey<String> b = base.withSubTaskIndex(1, 0);

    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void subtaskKeysDifferByAttemptId() {
    FeedbackKey<String> base = new FeedbackKey<>("p", 7L);

    SubtaskFeedbackKey<String> a = base.withSubTaskIndex(0, 0);
    SubtaskFeedbackKey<String> b = base.withSubTaskIndex(0, 1);

    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void subtaskKeyEqualsToSelfAndNotEqualsToNullOrOther() {
    SubtaskFeedbackKey<String> a = new FeedbackKey<String>("p", 1L).withSubTaskIndex(0, 0);

    assertThat(a).isEqualTo(a).isNotEqualTo(null).isNotEqualTo("string");
  }
}
