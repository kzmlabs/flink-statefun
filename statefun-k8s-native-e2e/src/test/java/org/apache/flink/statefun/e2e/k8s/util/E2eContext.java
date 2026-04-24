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
