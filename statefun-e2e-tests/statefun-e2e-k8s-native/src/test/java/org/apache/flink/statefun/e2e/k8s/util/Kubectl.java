// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.e2e.k8s.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Thin kubectl wrapper for E2E assertions, following the ProcessBuilder pattern of KubectlPortForward. */
public final class Kubectl {

  private Kubectl() {}

  /** Runs kubectl with the given args and returns stdout, failing the test on non-zero exit. */
  public static String run(String... args) {
    List<String> cmd = new ArrayList<>();
    cmd.add("kubectl");
    cmd.addAll(List.of(args));
    try {
      Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
      String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      if (!p.waitFor(120, TimeUnit.SECONDS) || p.exitValue() != 0) {
        throw new RuntimeException("kubectl " + String.join(" ", args) + " failed:\n" + out);
      }
      return out;
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("kubectl " + String.join(" ", args) + " failed", e);
    }
  }

  /** Current Flink job state of a FlinkDeployment CR, empty string while unset. */
  public static String jobState(String deployment) {
    return run("get", "flinkdeployment", deployment, "-n", E2eContext.NAMESPACE, "-o", "jsonpath={.status.jobStatus.state}").trim();
  }

  /**
   * JobManager log of the given FlinkDeployment, current container plus the previous one when the
   * pod has restarted. An application-mode JM exits on terminal job failure, so the diagnostics of
   * that failure live in the previous container's log.
   */
  public static String jobManagerLog(String deployment) {
    String selector = "app=" + deployment + ",component=jobmanager";
    String current = run("logs", "-n", E2eContext.NAMESPACE, "-l", selector, "--tail=-1");
    try {
      return current + run("logs", "-n", E2eContext.NAMESPACE, "-l", selector, "--tail=-1", "--previous");
    } catch (RuntimeException noPreviousContainer) {
      return current;
    }
  }

  /** Applies a classpath resource as a manifest via a temp file. */
  public static void apply(String classpathResource) {
    try (InputStream in = Kubectl.class.getClassLoader().getResourceAsStream(classpathResource)) {
      if (in == null) {
        throw new IllegalArgumentException("classpath resource not found: " + classpathResource);
      }
      Path tmp = Files.createTempFile("e2e-manifest-", ".yaml");
      Files.write(tmp, in.readAllBytes());
      run("apply", "-f", tmp.toString());
      Files.deleteIfExists(tmp);
    } catch (IOException e) {
      throw new RuntimeException("failed to apply " + classpathResource, e);
    }
  }

  /** Deletes a FlinkDeployment and waits for it to be gone; no-op when absent. */
  public static void deleteFlinkDeployment(String name) {
    run("delete", "flinkdeployment", name, "-n", E2eContext.NAMESPACE, "--ignore-not-found", "--wait=true");
  }

  /** Drops and recreates a topic on the in-cluster Kafka broker, discarding any poison records. */
  public static void recreateTopic(String topic) {
    String kafkaPod = run("get", "pod", "-n", E2eContext.NAMESPACE, "-l", "app=kafka", "-o", "jsonpath={.items[0].metadata.name}").trim();
    try {
      run("exec", "-n", E2eContext.NAMESPACE, kafkaPod, "--", "/opt/kafka/bin/kafka-topics.sh", "--delete", "--bootstrap-server", "localhost:9092", "--topic", topic);
    } catch (RuntimeException ignored) {
      // topic may not exist on the first reset; creation below is the part that matters
    }
    run("exec", "-n", E2eContext.NAMESPACE, kafkaPod, "--", "/opt/kafka/bin/kafka-topics.sh", "--create", "--if-not-exists", "--bootstrap-server", "localhost:9092", "--topic", topic, "--partitions", "1", "--replication-factor", "1");
  }
}
