// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.httpfn;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.statefun.extensions.ExtensionModule;
import org.apache.flink.statefun.flink.core.nettyclient.NettyRequestReplyClientFactory;
import org.apache.flink.statefun.flink.core.nettyclient.NettyTransportModule;
import org.apache.flink.statefun.sdk.TypeName;
import org.junit.jupiter.api.Test;

/**
 * Pins the auto-discovered transport-client factory bindings: OkHttp on the legacy
 * `okhttp-client` kind, Netty on the new `async-client` (default) kind. These bindings are what
 * `HttpFunctionEndpointSpec.transport.type` resolves against, so a regression here silently
 * routes user traffic through the wrong client.
 */
class TransportClientModulesTest {

  @Test
  void transportClientsModuleBindsDefaultHttpFactoryToOkHttpClientKind() {
    RecordingBinder binder = new RecordingBinder();

    new TransportClientsModule().configure(Collections.emptyMap(), binder);

    assertThat(binder.bindings).hasSize(1);
    assertThat(binder.bindings).containsKey(TransportClientConstants.OKHTTP_CLIENT_FACTORY_TYPE);
    assertThat(binder.bindings.get(TransportClientConstants.OKHTTP_CLIENT_FACTORY_TYPE))
        .isSameAs(DefaultHttpRequestReplyClientFactory.INSTANCE);
  }

  @Test
  void nettyTransportModuleBindsAsyncClientKindToNettyFactory() {
    RecordingBinder binder = new RecordingBinder();

    new NettyTransportModule().configure(Collections.emptyMap(), binder);

    assertThat(binder.bindings).hasSize(1);
    assertThat(binder.bindings).containsKey(TransportClientConstants.ASYNC_CLIENT_FACTORY_TYPE);
    assertThat(binder.bindings.get(TransportClientConstants.ASYNC_CLIENT_FACTORY_TYPE))
        .isSameAs(NettyRequestReplyClientFactory.INSTANCE);
  }

  @Test
  void okhttpAndAsyncKindsAreDistinct() {
    // Pin: OKHTTP_CLIENT_FACTORY_TYPE and ASYNC_CLIENT_FACTORY_TYPE are different TypeNames so a
    // single map can hold both bindings without collision.
    assertThat(TransportClientConstants.OKHTTP_CLIENT_FACTORY_TYPE)
        .isNotEqualTo(TransportClientConstants.ASYNC_CLIENT_FACTORY_TYPE);
  }

  private static final class RecordingBinder implements ExtensionModule.Binder {
    final Map<TypeName, Object> bindings = new HashMap<>();

    @Override
    public <T> void bindExtension(TypeName extensionType, T extension) {
      bindings.put(extensionType, extension);
    }
  }
}
