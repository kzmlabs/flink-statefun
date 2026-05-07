// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.httpfn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import okhttp3.OkHttpClient;
import org.junit.jupiter.api.Test;

class OkHttpUtilsTest {

  @Test
  void newClientHasUnboundedDispatcherAndPositiveConnectionPool() {
    OkHttpClient client = OkHttpUtils.newClient();
    try {
      // Dispatcher must be set high enough to never throttle StateFun's invocation fanout.
      assertThat(client.dispatcher().getMaxRequests()).isEqualTo(Integer.MAX_VALUE);
      assertThat(client.dispatcher().getMaxRequestsPerHost()).isEqualTo(Integer.MAX_VALUE);
      // Pool capacity is 1024; pin it so a future shrink to a lower value gets noticed.
      assertThat(client.connectionPool().connectionCount()).isZero();
      assertThat(client.followRedirects()).isTrue();
      assertThat(client.followSslRedirects()).isTrue();
      assertThat(client.retryOnConnectionFailure()).isTrue();
    } finally {
      OkHttpUtils.closeSilently(client);
    }
  }

  @Test
  void closeSilentlyOnNullIsNoOp() {
    assertThatCode(() -> OkHttpUtils.closeSilently(null)).doesNotThrowAnyException();
  }

  @Test
  void closeSilentlyOnRunningClientShutsDownDispatcherExecutor() {
    OkHttpClient client = OkHttpUtils.newClient();

    OkHttpUtils.closeSilently(client);

    assertThat(client.dispatcher().executorService().isShutdown()).isTrue();
  }

  @Test
  void closeSilentlyIsIdempotent() {
    OkHttpClient client = OkHttpUtils.newClient();

    OkHttpUtils.closeSilently(client);

    // Pin: a second close on the same client must NOT throw — closeSilently swallows all
    // throwables from already-closed dispatchers/pools by design.
    assertThatCode(() -> OkHttpUtils.closeSilently(client)).doesNotThrowAnyException();
  }
}
