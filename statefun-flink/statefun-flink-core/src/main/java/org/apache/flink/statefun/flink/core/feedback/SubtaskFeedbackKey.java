// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.feedback;

import java.io.Serializable;
import java.util.Objects;

/** A FeedbackKey bounded to a subtask index. */
@SuppressWarnings("unused")
public final class SubtaskFeedbackKey<V> implements Serializable {

  private static final long serialVersionUID = 1;

  private final String pipelineName;
  private final int subtaskIndex;
  private final long invocationId;
  private final int attemptId;

  SubtaskFeedbackKey(String pipeline, long invocationId, int subtaskIndex, int attemptId) {
    this.pipelineName = Objects.requireNonNull(pipeline);
    this.invocationId = invocationId;
    this.subtaskIndex = subtaskIndex;
    this.attemptId = attemptId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SubtaskFeedbackKey<?> that = (SubtaskFeedbackKey<?>) o;
    return subtaskIndex == that.subtaskIndex
        && invocationId == that.invocationId
        && attemptId == that.attemptId
        && Objects.equals(pipelineName, that.pipelineName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipelineName, subtaskIndex, invocationId, attemptId);
  }
}
