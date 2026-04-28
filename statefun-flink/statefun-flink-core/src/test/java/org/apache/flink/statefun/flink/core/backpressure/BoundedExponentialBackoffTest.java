// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.backpressure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;

import java.time.Duration;
import org.junit.jupiter.api.Test;

public class BoundedExponentialBackoffTest {
  private final FakeNanoClock fakeTime = new FakeNanoClock();
  private final BoundedExponentialBackoff backoffUnderTest =
      new BoundedExponentialBackoff(fakeTime, Duration.ofSeconds(1), Duration.ofMinutes(1));

  @Test
  public void simpleUsage() {
    assertThat(backoffUnderTest.applyNow(), is(true));
    assertThat(fakeTime.now(), greaterThan(0L));
  }

  @Test
  public void timeoutExpired() {
    fakeTime.now = Duration.ofMinutes(1).toNanos();
    assertThat(backoffUnderTest.applyNow(), is(false));
  }

  @Test
  @SuppressWarnings("StatementWithEmptyBody")
  public void totalNumberOfBackoffsIsEqualToTimeout() {
    while (backoffUnderTest.applyNow()) {}

    assertThat(fakeTime.now(), is(Duration.ofMinutes(1).toNanos()));
  }

  private static final class FakeNanoClock implements Timer {
    long now;

    @Override
    public long now() {
      return now;
    }

    @Override
    public void sleep(long durationNano) {
      now += durationNano;
    }
  }
}
