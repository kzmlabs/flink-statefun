// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.feedback;

import java.io.OutputStream;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Supplier;
import org.apache.flink.statefun.flink.core.logger.FeedbackLogger;
import org.apache.flink.util.IOUtils;

final class Checkpoints<T> implements AutoCloseable {
  private final Supplier<? extends FeedbackLogger<T>> feedbackLoggerFactory;
  private final TreeMap<Long, FeedbackLogger<T>> uncompletedCheckpoints = new TreeMap<>();

  Checkpoints(Supplier<? extends FeedbackLogger<T>> feedbackLoggerFactory) {
    this.feedbackLoggerFactory = Objects.requireNonNull(feedbackLoggerFactory);
  }

  public void startLogging(long checkpointId, OutputStream outputStream) {
    FeedbackLogger<T> logger = feedbackLoggerFactory.get();
    logger.startLogging(outputStream);
    uncompletedCheckpoints.put(checkpointId, logger);
  }

  public void append(T element) {
    for (FeedbackLogger<T> logger : uncompletedCheckpoints.values()) {
      logger.append(element);
    }
  }

  public void commitCheckpointsUntil(long checkpointId) {
    SortedMap<Long, FeedbackLogger<T>> completedCheckpoints =
        uncompletedCheckpoints.headMap(checkpointId, true);
    completedCheckpoints.values().forEach(FeedbackLogger::commit);
    completedCheckpoints.clear();
  }

  @Override
  public void close() {
    IOUtils.closeAllQuietly(uncompletedCheckpoints.values());
    uncompletedCheckpoints.clear();
  }
}
