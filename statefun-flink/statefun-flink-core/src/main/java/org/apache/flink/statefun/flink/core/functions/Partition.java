// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.functions;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.assignKeyToParallelOperator;

import org.apache.flink.statefun.flink.core.common.KeyBy;
import org.apache.flink.statefun.sdk.Address;

class Partition {
  private final int maxParallelism;
  private final int parallelism;
  private final int thisOperatorIndex;

  Partition(int maxParallelism, int parallelism, int thisOperatorIndex) {
    this.maxParallelism = maxParallelism;
    this.parallelism = parallelism;
    this.thisOperatorIndex = thisOperatorIndex;
  }

  boolean contains(Address address) {
    final int destinationOperatorIndex =
        assignKeyToParallelOperator(KeyBy.apply(address), maxParallelism, parallelism);
    return thisOperatorIndex == destinationOperatorIndex;
  }
}
