// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.feedback;

import java.util.Deque;

/**
 * HandOffQueue - is a single producer single consumer (spsc) queue that supports adding and
 * draining atomically.
 *
 * <p>Implementors of this queue supports atomic addition operation (via {@link
 * #addAndCheckIfWasEmpty(Object)} and atomic, bulk retrieving of the content of this queue (via
 * {@link #drainAll()})}.
 *
 * @param <ElementT> element type that is stored in this queue.
 */
interface FeedbackQueue<ElementT> {

  /**
   * Adds an element to the queue atomically.
   *
   * @param element the element to add to the queue.
   * @return true, if prior to this addition the queue was empty.
   */
  boolean addAndCheckIfWasEmpty(ElementT element);

  /**
   * Atomically grabs all that elements of this queue.
   *
   * @return the elements present at the queue at the moment of this operation.
   */
  Deque<ElementT> drainAll();
}
