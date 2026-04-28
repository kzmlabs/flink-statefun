// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.feedback;

/** HandOffConsumer. */
@FunctionalInterface
public interface FeedbackConsumer<T> {

  void processFeedback(T element) throws Exception;
}
