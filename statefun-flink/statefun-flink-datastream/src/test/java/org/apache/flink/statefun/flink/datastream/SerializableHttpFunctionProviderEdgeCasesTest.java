// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.datastream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.statefun.sdk.TypeName;
import org.junit.jupiter.api.Test;

class SerializableHttpFunctionProviderEdgeCasesTest {

  @Test
  void getClientFactoryRejectsUnknownTransportType() {
    TypeName unknown = TypeName.parseFrom("io.test/unknown-transport");

    assertThatThrownBy(() -> SerializableHttpFunctionProvider.getClientFactory(unknown))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Unsupported transport client factory type")
        .hasMessageContaining("io.test/unknown-transport");
  }

  @Test
  void getClientFactoryRejectsNullTransportType() {
    // The mapper short-circuits null via .equals — we want it to fail fast rather than
    // silently fall through to "unsupported" which would mask null-safety bugs upstream.
    assertThatThrownBy(() -> SerializableHttpFunctionProvider.getClientFactory(null))
        .isInstanceOfAny(NullPointerException.class, UnsupportedOperationException.class);
  }

  @Test
  void exposedFactoriesAreNotNull() {
    // The two SDK-supported transports must always resolve to a non-null factory; otherwise
    // the JobGraph would later fail with a confusing NullPointerException at runtime.
    assertThat(
            SerializableHttpFunctionProvider.getClientFactory(
                org.apache.flink.statefun.flink.core.httpfn.TransportClientConstants
                    .OKHTTP_CLIENT_FACTORY_TYPE))
        .isNotNull();
    assertThat(
            SerializableHttpFunctionProvider.getClientFactory(
                org.apache.flink.statefun.flink.core.httpfn.TransportClientConstants
                    .ASYNC_CLIENT_FACTORY_TYPE))
        .isNotNull();
  }
}
