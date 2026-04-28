// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.harness.io;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.flink.api.connector.source.SourceSplit;

public class SupplyingSourceSplit<T> implements SourceSplit, Serializable {
  private static final long serialVersionUID = 1L;

  private final int id;
  private final BlockingQueue<T> records;

  public SupplyingSourceSplit(int id) {
    this.id = id;
    this.records = new LinkedBlockingQueue<>();
  }

  @Override
  public String splitId() {
    return Integer.toString(id);
  }

  /** Get the next element. Block if asked. */
  public T getNext(boolean blocking) throws InterruptedException {
    return blocking ? records.take() : records.poll();
  }

  /** Add a record to this split. */
  public SupplyingSourceSplit<T> addRecord(T record) {
    if (!records.offer(record)) {
      throw new IllegalStateException("Failed to add record to split.");
    }
    return this;
  }
}
