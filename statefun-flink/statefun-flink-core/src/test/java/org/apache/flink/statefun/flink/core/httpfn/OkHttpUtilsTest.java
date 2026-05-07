// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.httpfn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import okhttp3.OkHttpClient;
import org.junit.jupiter.api.Test;

class OkHttpUtilsTest {

  @Test
  void newClientHasUnboundedDispatcherAndIdleConnectionPool() {
    OkHttpClient client = OkHttpUtils.newClient();
    try {
      assertThat(client.dispatcher().getMaxRequests()).isEqualTo(Integer.MAX_VALUE);
      assertThat(client.dispatcher().getMaxRequestsPerHost()).isEqualTo(Integer.MAX_VALUE);
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

    assertThatCode(() -> OkHttpUtils.closeSilently(client)).doesNotThrowAnyException();
  }
}
