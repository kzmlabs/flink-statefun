// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.feedback;

/** HandOffConsumer. */
@FunctionalInterface
public interface FeedbackConsumer<T> {

  void processFeedback(T element) throws Exception;
}
