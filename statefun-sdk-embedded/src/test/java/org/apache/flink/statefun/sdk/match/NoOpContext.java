// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.match;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.metrics.Metrics;

/** No-op {@link Context} stub for tests that do not exercise context interactions. */
final class NoOpContext implements Context {
  @Override
  public Address self() {
    return null;
  }

  @Override
  public Address caller() {
    return null;
  }

  @Override
  public void send(Address to, Object message) {}

  @Override
  public <T> void send(EgressIdentifier<T> egress, T message) {}

  @Override
  public void sendAfter(Duration delay, Address to, Object message) {}

  @Override
  public void sendAfter(Duration delay, Address to, Object message, String cancellationToken) {}

  @Override
  public void cancelDelayedMessage(String cancellationToken) {}

  @Override
  public <M, T> void registerAsyncOperation(M metadata, CompletableFuture<T> future) {}

  @Override
  public Metrics metrics() {
    return null;
  }
}
