// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.e2e.smoke.embedded;

import static org.apache.flink.statefun.e2e.smoke.SmokeRunner.awaitVerificationSuccess;

import java.util.concurrent.TimeUnit;
import org.apache.flink.statefun.e2e.smoke.SimpleVerificationServer;
import org.apache.flink.statefun.e2e.smoke.SmokeRunnerParameters;
import org.apache.flink.statefun.flink.harness.Harness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedSmokeHarnessTest {

  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedSmokeHarnessTest.class);

  @Test
  @Timeout(value = 1_000 * 60, unit = TimeUnit.MILLISECONDS)
  public void miniClusterTest() throws Exception {
    Harness harness = new Harness();

    // set Flink related configuration.
    harness.withConfiguration(
        "classloader.parent-first-patterns.additional",
        "org.apache.flink.statefun;org.apache.kafka;com.google.protobuf");
    harness.withConfiguration("execution.restart-strategy.type", "fixed-delay");
    harness.withConfiguration("execution.restart-strategy.fixed-delay.attempts", "2147483647");
    harness.withConfiguration("execution.restart-strategy.fixed-delay.delay", "1sec");
    harness.withConfiguration("execution.checkpointing.interval", "2sec");
    harness.withConfiguration("execution.checkpointing.mode", "EXACTLY_ONCE");
    harness.withConfiguration("execution.checkpointing.max-concurrent-checkpoints", "3");
    harness.withConfiguration("parallelism.default", "1");
    harness.withConfiguration("state.checkpoints.dir", "file:///tmp/checkpoints");

    // start the verification server
    SimpleVerificationServer.StartedServer started = new SimpleVerificationServer().start();

    // configure test parameters.
    SmokeRunnerParameters parameters = new SmokeRunnerParameters();
    parameters.setMaxFailures(0);
    parameters.setMessageCount(10_000);
    parameters.setNumberOfFunctionInstances(32);
    parameters.setVerificationServerHost("localhost");
    parameters.setVerificationServerPort(started.port());
    parameters.setAsyncOpSupported(true);
    parameters.setDelayCancellationOpSupported(true);
    parameters.asMap().forEach(harness::withGlobalConfiguration);

    // run the harness.
    try (AutoCloseable ignored = startHarnessInTheBackground(harness)) {
      awaitVerificationSuccess(started.results(), parameters.getNumberOfFunctionInstances());
    }

    LOG.info("All done.");
  }

  private static AutoCloseable startHarnessInTheBackground(Harness harness) {
    Thread t =
        new Thread(
            () -> {
              try {
                harness.start();
              } catch (InterruptedException ignored) {
                LOG.info("Harness Thread was interrupted. Exiting...");
              } catch (Exception exception) {
                LOG.info("Something happened while trying to run the Harness.", exception);
              }
            });
    t.setName("harness-runner");
    t.setDaemon(true);
    t.start();
    return t::interrupt;
  }
}
