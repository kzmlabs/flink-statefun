/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.statefun.e2e.k8s.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Spawns and manages a {@code kubectl port-forward} subprocess for test-side access to in-cluster
 * services. Supports both fixed-port and ephemeral-port forwarding.
 */
public final class KubectlPortForward implements AutoCloseable {

  private static final Pattern FORWARDING_LINE =
      Pattern.compile("Forwarding from 127\\.0\\.0\\.1:(\\d+)");
  private static final Duration READY_TIMEOUT = Duration.ofSeconds(30);

  private final Process process;
  private final int localPort;

  private KubectlPortForward(Process process, int localPort) {
    this.process = process;
    this.localPort = localPort;
  }

  /**
   * Starts a port-forward to a fixed local port. The method blocks until the port is reachable.
   *
   * @throws IllegalStateException if the port does not become reachable within {@link
   *     #READY_TIMEOUT}.
   */
  public static KubectlPortForward fixed(
      String namespace, String resource, int localPort, int remotePort) throws Exception {
    Process process = spawn(namespace, resource, localPort + ":" + remotePort, logFile(localPort));
    awaitPortOpen(localPort);
    return new KubectlPortForward(process, localPort);
  }

  /**
   * Starts a port-forward to an ephemeral local port and returns once reachable. The OS-assigned
   * port is parsed from kubectl's stdout.
   */
  public static KubectlPortForward ephemeral(String namespace, String resource, int remotePort)
      throws Exception {
    Process process = spawn(namespace, resource, "0:" + remotePort, null);
    int localPort = parseAssignedPort(process);
    awaitPortOpen(localPort);
    return new KubectlPortForward(process, localPort);
  }

  public int localPort() {
    return localPort;
  }

  @Override
  public void close() {
    process.destroyForcibly();
    try {
      process.waitFor(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static Process spawn(
      String namespace, String resource, String portSpec, File optionalLogFile) throws Exception {
    ProcessBuilder pb =
        new ProcessBuilder(
            List.of(
                "kubectl",
                "port-forward",
                "-n",
                namespace,
                "--address",
                "127.0.0.1",
                resource,
                portSpec));
    pb.redirectErrorStream(true);
    if (optionalLogFile != null) {
      pb.redirectOutput(optionalLogFile);
    }
    return pb.start();
  }

  private static int parseAssignedPort(Process process) throws Exception {
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
    long deadline = System.currentTimeMillis() + READY_TIMEOUT.toMillis();
    while (System.currentTimeMillis() < deadline) {
      String line = reader.readLine();
      if (line == null) {
        Thread.sleep(100);
        continue;
      }
      Matcher m = FORWARDING_LINE.matcher(line);
      if (m.find()) {
        int port = Integer.parseInt(m.group(1));
        // Drain remaining output in a daemon thread so the process doesn't block.
        Thread drainer = new Thread(() -> drain(reader), "kubectl-pf-drain-" + port);
        drainer.setDaemon(true);
        drainer.start();
        return port;
      }
    }
    throw new IllegalStateException(
        "kubectl port-forward did not report an assigned port within " + READY_TIMEOUT);
  }

  private static void drain(BufferedReader reader) {
    try {
      while (reader.readLine() != null) {
        // discard
      }
    } catch (Exception ignored) {
      // stream closed
    }
  }

  private static void awaitPortOpen(int port) throws Exception {
    long deadline = System.currentTimeMillis() + READY_TIMEOUT.toMillis();
    while (System.currentTimeMillis() < deadline) {
      try (Socket s = new Socket(InetAddress.getByName("127.0.0.1"), port)) {
        return;
      } catch (Exception ignored) {
        Thread.sleep(500);
      }
    }
    throw new IllegalStateException(
        "Port 127.0.0.1:" + port + " did not open within " + READY_TIMEOUT);
  }

  private static File logFile(int port) {
    return new File("target/port-forward-" + port + ".log");
  }
}
