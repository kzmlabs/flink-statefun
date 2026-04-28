// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.e2e.smoke;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.flink.statefun.e2e.common.StatefulFunctionsAppContainers;
import org.apache.flink.statefun.e2e.smoke.generated.VerificationResult;
import org.apache.flink.util.function.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;

public final class SmokeRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SmokeRunner.class);

  public static void run(
      SmokeRunnerParameters parameters, StatefulFunctionsAppContainers.Builder builder)
      throws Throwable {
    // start verification server
    SimpleVerificationServer.StartedServer server = new SimpleVerificationServer().start();
    parameters.setVerificationServerHost("host.testcontainers.internal");
    parameters.setVerificationServerPort(server.port());
    Testcontainers.exposeHostPorts(server.port());

    // set the test module parameters as global configurations, so that
    // it can be deserialized at Module#configure()
    parameters.asMap().forEach(builder::withModuleGlobalConfiguration);
    builder.exposeLogs(LOG);
    StatefulFunctionsAppContainers app = builder.build();

    // run the test
    run(
        app,
        () ->
            awaitVerificationSuccess(server.results(), parameters.getNumberOfFunctionInstances()));
  }

  private static void run(StatefulFunctionsAppContainers app, ThrowingRunnable<Throwable> r)
      throws Throwable {
    try {
      app.beforeAll(null);
      r.run();
    } finally {
      app.afterAll(null);
    }
  }

  public static void awaitVerificationSuccess(
      Supplier<VerificationResult> results, final int numberOfFunctionInstances) {
    Set<Integer> successfullyVerified = new HashSet<>();
    while (successfullyVerified.size() != numberOfFunctionInstances) {
      VerificationResult result = results.get();
      if (result.getActual() == result.getExpected()) {
        successfullyVerified.add(result.getId());
      } else if (result.getActual() > result.getExpected()) {
        throw new AssertionError(
            "Over counted. Expected: "
                + result.getExpected()
                + ", actual: "
                + result.getActual()
                + ", function: "
                + result.getId());
      }
    }
  }
}
