// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.metrics;

/** A Counter. */
public interface Counter {

  /**
   * Increment the amount of this counter by @amount;
   *
   * @param amount the amount to increment by;
   */
  void inc(long amount);

  /**
   * Decrement the amount of this counter by @amount;
   *
   * @param amount the amount to increment by;
   */
  void dec(long amount);

  /** Increment the value of this counter by 1. */
  default void inc() {
    inc(1);
  }

  /** Decrement the value of this counter by 1. */
  default void dec() {
    dec(1);
  }
}
