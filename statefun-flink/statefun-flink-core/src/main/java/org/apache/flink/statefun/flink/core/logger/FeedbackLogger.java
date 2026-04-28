// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.logger;

import java.io.OutputStream;

public interface FeedbackLogger<T> extends AutoCloseable {

  /** Start logging messages into the supplied output stream. */
  void startLogging(OutputStream keyedStateCheckpointOutputStream);

  /** Append a message to the currently logging logger. */
  void append(T message);

  /** Commit the currently logging logger. */
  void commit();
}
