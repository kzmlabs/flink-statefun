// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.e2e.k8s.util;

import java.time.Duration;

/** Shared constants for the K8s native E2E suite. */
public final class E2eContext {

  private E2eContext() {}

  /** Namespace that {@code scripts/setup-cluster.sh} provisions. */
  public static final String NAMESPACE = "statefun-e2e";

  /** Outer deadline for awaitility polls — Flink is event-time bound and LocalStack is slow. */
  public static final Duration POLL_TIMEOUT = Duration.ofMinutes(3);

  /** Inner poll interval — frequent enough to catch checkpoints, cheap enough not to DoS. */
  public static final Duration POLL_INTERVAL = Duration.ofSeconds(2);
}
