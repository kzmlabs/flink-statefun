// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.backpressure;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.statefun.flink.core.TestUtils;
import org.junit.jupiter.api.Test;

public class ThresholdBackPressureValveTest {

  @Test
  public void simpleUsage() {
    ThresholdBackPressureValve valve = new ThresholdBackPressureValve(2);

    valve.notifyAsyncOperationRegistered();
    valve.notifyAsyncOperationRegistered();

    assertTrue(valve.shouldBackPressure());
  }

  @Test
  public void completedOperationReleaseBackpressure() {
    ThresholdBackPressureValve valve = new ThresholdBackPressureValve(1);

    valve.notifyAsyncOperationRegistered();
    valve.notifyAsyncOperationCompleted(TestUtils.FUNCTION_1_ADDR);

    assertFalse(valve.shouldBackPressure());
  }

  @Test
  public void blockAddressTriggerBackpressure() {
    ThresholdBackPressureValve valve = new ThresholdBackPressureValve(500);

    valve.blockAddress(TestUtils.FUNCTION_1_ADDR);

    assertTrue(valve.shouldBackPressure());
  }

  @Test
  public void blockingAndUnblockingAddress() {
    ThresholdBackPressureValve valve = new ThresholdBackPressureValve(500);

    valve.blockAddress(TestUtils.FUNCTION_1_ADDR);
    valve.notifyAsyncOperationCompleted(TestUtils.FUNCTION_1_ADDR);

    assertFalse(valve.shouldBackPressure());
  }

  @Test
  public void unblockingDifferentAddressStillBackpressures() {
    ThresholdBackPressureValve valve = new ThresholdBackPressureValve(500);

    valve.blockAddress(TestUtils.FUNCTION_1_ADDR);
    valve.notifyAsyncOperationCompleted(TestUtils.FUNCTION_2_ADDR);

    assertTrue(valve.shouldBackPressure());
  }

  @Test
  public void blockTwoAddress() {
    ThresholdBackPressureValve valve = new ThresholdBackPressureValve(500);

    valve.blockAddress(TestUtils.FUNCTION_1_ADDR);
    valve.blockAddress(TestUtils.FUNCTION_2_ADDR);
    assertTrue(valve.shouldBackPressure());

    valve.notifyAsyncOperationCompleted(TestUtils.FUNCTION_1_ADDR);
    assertTrue(valve.shouldBackPressure());

    valve.notifyAsyncOperationCompleted(TestUtils.FUNCTION_2_ADDR);
    assertFalse(valve.shouldBackPressure());
  }
}
