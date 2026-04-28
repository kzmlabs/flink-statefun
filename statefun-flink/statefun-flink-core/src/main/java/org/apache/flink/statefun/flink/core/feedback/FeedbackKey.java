// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.feedback;

import java.io.Serializable;
import java.util.Objects;

/** A FeedbackKey without runtime information. */
public final class FeedbackKey<V> implements Serializable {

  private static final long serialVersionUID = 1;

  private final String pipelineName;
  private final long invocationId;

  public FeedbackKey(String pipelineName, long invocationId) {
    this.pipelineName = Objects.requireNonNull(pipelineName);
    this.invocationId = invocationId;
  }

  public SubtaskFeedbackKey<V> withSubTaskIndex(int subTaskIndex, int attemptId) {
    return new SubtaskFeedbackKey<>(pipelineName, invocationId, subTaskIndex, attemptId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FeedbackKey<?> that = (FeedbackKey<?>) o;
    return invocationId == that.invocationId && Objects.equals(pipelineName, that.pipelineName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipelineName, invocationId);
  }

  public String asColocationKey() {
    return String.format("CO-LOCATION/%s/%d", pipelineName, invocationId);
  }
}
