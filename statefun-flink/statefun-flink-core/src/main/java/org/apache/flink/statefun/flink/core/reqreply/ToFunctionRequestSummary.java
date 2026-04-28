// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.reqreply;

import java.util.Objects;
import org.apache.flink.statefun.sdk.Address;

/**
 * ToFunctionRequestSummary - represents a summary of that request, it is indented to be used as an
 * additional context for logging.
 */
public final class ToFunctionRequestSummary {
  private final Address address;
  private final int batchSize;
  private final int totalSizeInBytes;
  private final int numberOfStates;

  public ToFunctionRequestSummary(
      Address address, int totalSizeInBytes, int numberOfStates, int batchSize) {
    this.address = Objects.requireNonNull(address);
    this.totalSizeInBytes = totalSizeInBytes;
    this.numberOfStates = numberOfStates;
    this.batchSize = batchSize;
  }

  @Override
  public String toString() {
    return "ToFunctionRequestSummary("
        + "address="
        + address
        + ", batchSize="
        + batchSize
        + ", totalSizeInBytes="
        + totalSizeInBytes
        + ", numberOfStates="
        + numberOfStates
        + ')';
  }
}
