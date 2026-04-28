// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.e2e.smoke.embedded;

import java.util.concurrent.TimeUnit;
import org.apache.flink.statefun.e2e.common.StatefulFunctionsAppContainers;
import org.apache.flink.statefun.e2e.smoke.SmokeRunner;
import org.apache.flink.statefun.e2e.smoke.SmokeRunnerParameters;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class SmokeVerificationEmbeddedE2E {

  private static final int NUM_WORKERS = 2;

  @Test
  @Timeout(value = 1_000 * 60 * 10, unit = TimeUnit.MILLISECONDS)
  public void run() throws Throwable {
    SmokeRunnerParameters parameters = new SmokeRunnerParameters();
    parameters.setNumberOfFunctionInstances(128);
    parameters.setMessageCount(100_000);
    parameters.setMaxFailures(1);
    parameters.setAsyncOpSupported(true);
    parameters.setDelayCancellationOpSupported(true);

    StatefulFunctionsAppContainers.Builder builder =
        StatefulFunctionsAppContainers.builder("flink-statefun-cluster", NUM_WORKERS);

    SmokeRunner.run(parameters, builder);
  }
}
