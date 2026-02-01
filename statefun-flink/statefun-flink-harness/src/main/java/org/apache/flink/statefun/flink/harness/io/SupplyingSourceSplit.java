/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
