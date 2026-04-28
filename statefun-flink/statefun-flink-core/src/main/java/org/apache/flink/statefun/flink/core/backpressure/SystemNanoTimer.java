// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.backpressure;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/** A {@code Timer} backed by {@link System#nanoTime()}. */
final class SystemNanoTimer implements Timer {
  private static final SystemNanoTimer INSTANCE = new SystemNanoTimer();

  public static SystemNanoTimer instance() {
    return INSTANCE;
  }

  private SystemNanoTimer() {}

  @Override
  public long now() {
    return System.nanoTime();
  }

  @Override
  public void sleep(long sleepTimeNanos) {
    try {
      final long sleepTimeMs = NANOSECONDS.toMillis(sleepTimeNanos);
      Thread.sleep(sleepTimeMs);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("interrupted while sleeping", ex);
    }
  }
}
