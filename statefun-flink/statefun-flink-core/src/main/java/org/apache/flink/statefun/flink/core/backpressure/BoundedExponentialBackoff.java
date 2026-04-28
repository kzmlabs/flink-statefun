// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.backpressure;

import java.time.Duration;
import java.util.Objects;
import org.apache.flink.annotation.VisibleForTesting;

public final class BoundedExponentialBackoff {
  private final Timer timer;
  private final long requestStartTimeInNanos;
  private final long maxRequestDurationInNanos;

  private long nextSleepTimeNanos;

  public BoundedExponentialBackoff(Duration initialBackoffDuration, Duration maxRequestDuration) {
    this(SystemNanoTimer.instance(), initialBackoffDuration, maxRequestDuration);
  }

  @VisibleForTesting
  BoundedExponentialBackoff(
      Timer timer, Duration initialBackoffDuration, Duration maxRequestDuration) {
    this.timer = Objects.requireNonNull(timer);
    this.requestStartTimeInNanos = timer.now();
    this.maxRequestDurationInNanos = maxRequestDuration.toNanos();
    this.nextSleepTimeNanos = initialBackoffDuration.toNanos();
  }

  public boolean applyNow() {
    final long remainingNanos = remainingNanosUntilDeadLine();
    final long nextAmountOfNanosToSleep = nextAmountOfNanosToSleep();
    final long actualSleep = Math.min(remainingNanos, nextAmountOfNanosToSleep);
    if (actualSleep <= 0) {
      return false;
    }
    timer.sleep(actualSleep);
    return true;
  }

  private long remainingNanosUntilDeadLine() {
    final long totalElapsedTime = timer.now() - requestStartTimeInNanos;
    return maxRequestDurationInNanos - totalElapsedTime;
  }

  private long nextAmountOfNanosToSleep() {
    final long current = nextSleepTimeNanos;
    nextSleepTimeNanos *= 2;
    return current;
  }
}
