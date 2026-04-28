// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.e2e.smoke.js;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import org.apache.flink.statefun.e2e.common.StatefulFunctionsAppContainers;
import org.apache.flink.statefun.e2e.smoke.SmokeRunner;
import org.apache.flink.statefun.e2e.smoke.SmokeRunnerParameters;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;

public class SmokeVerificationJsE2E {

  private static final Logger LOG = LoggerFactory.getLogger(SmokeVerificationJsE2E.class);
  private static final int NUM_WORKERS = 2;

  @Test
  @Timeout(value = 1_000 * 60 * 10, unit = TimeUnit.MILLISECONDS)
  public void runWith() throws Throwable {
    SmokeRunnerParameters parameters = new SmokeRunnerParameters();
    parameters.setNumberOfFunctionInstances(128);
    parameters.setMessageCount(100_000);
    parameters.setMaxFailures(1);

    GenericContainer<?> remoteFunction = configureRemoteFunction();

    StatefulFunctionsAppContainers.Builder builder =
        StatefulFunctionsAppContainers.builder("flink-statefun-cluster", NUM_WORKERS)
            .withBuildContextFileFromClasspath("remote-module", "/remote-module/")
            .dependsOn(remoteFunction);

    SmokeRunner.run(parameters, builder);
  }

  private GenericContainer<?> configureRemoteFunction() {
    ImageFromDockerfile remoteFunctionImage =
        new ImageFromDockerfile("remote-function-image")
            .withFileFromClasspath("Dockerfile", "Dockerfile.remote-function")
            .withFileFromPath("sdk", sdkPath())
            .withFileFromClasspath("remote-function/", "remote-function/");

    return new GenericContainer<>(remoteFunctionImage)
        .withNetworkAliases("remote-function-host")
        .withLogConsumer(new Slf4jLogConsumer(LOG));
  }

  private static Path sdkPath() {
    return Paths.get(System.getProperty("user.dir") + "/../../statefun-sdk-js");
  }
}
