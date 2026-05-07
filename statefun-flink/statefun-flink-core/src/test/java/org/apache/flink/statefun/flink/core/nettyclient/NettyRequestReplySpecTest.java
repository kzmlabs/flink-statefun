// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.nettyclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class NettyRequestReplySpecTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void allNullArgsFallBackToProductionDefaults() {
    NettyRequestReplySpec spec =
        new NettyRequestReplySpec(null, null, null, null, null, null, null, null, null, null);

    assertThat(spec.callTimeout).isEqualTo(NettyRequestReplySpec.DEFAULT_CALL_TIMEOUT);
    assertThat(spec.connectTimeout).isEqualTo(NettyRequestReplySpec.DEFAULT_CONNECT_TIMEOUT);
    assertThat(spec.pooledConnectionTTL)
        .isEqualTo(NettyRequestReplySpec.DEFAULT_POOLED_CONNECTION_TTL);
    assertThat(spec.connectionPoolMaxSize)
        .isEqualTo(NettyRequestReplySpec.DEFAULT_CONNECTION_POOL_MAX_SIZE);
    assertThat(spec.maxRequestOrResponseSizeInBytes)
        .isEqualTo(NettyRequestReplySpec.DEFAULT_MAX_REQUEST_OR_RESPONSE_SIZE_IN_BYTES);
    assertThat(spec.getTrustedCaCerts()).isEmpty();
    assertThat(spec.getClientCerts()).isEmpty();
    assertThat(spec.getClientKey()).isEmpty();
    assertThat(spec.getClientKeyPassword()).isEmpty();
  }

  @Test
  void explicitTopLevelTimeoutsTakeEffect() {
    NettyRequestReplySpec spec =
        new NettyRequestReplySpec(
            Duration.ofMinutes(3),
            Duration.ofSeconds(5),
            Duration.ofMinutes(1),
            512,
            16 * 1024 * 1024,
            null,
            null,
            null,
            null,
            null);

    assertThat(spec.callTimeout).isEqualTo(Duration.ofMinutes(3));
    assertThat(spec.connectTimeout).isEqualTo(Duration.ofSeconds(5));
    assertThat(spec.pooledConnectionTTL).isEqualTo(Duration.ofMinutes(1));
    assertThat(spec.connectionPoolMaxSize).isEqualTo(512);
    assertThat(spec.maxRequestOrResponseSizeInBytes).isEqualTo(16 * 1024 * 1024);
  }

  @Test
  void nestedTimeoutsObjectWinsOverNullTopLevelTimeouts() {
    NettyRequestReplySpec.Timeouts t = new NettyRequestReplySpec.Timeouts();
    t.setCallTimeout(Duration.ofMinutes(7));
    t.setConnectTimeout(Duration.ofSeconds(11));

    NettyRequestReplySpec spec =
        new NettyRequestReplySpec(null, null, null, null, null, null, null, null, null, t);

    // Pin the priority order: Timeouts > top-level > default.
    assertThat(spec.callTimeout).isEqualTo(Duration.ofMinutes(7));
    assertThat(spec.connectTimeout).isEqualTo(Duration.ofSeconds(11));
  }

  @Test
  void nestedTimeoutsObjectWinsOverTopLevelTimeoutsToo() {
    // When BOTH the legacy Timeouts object and the new top-level fields are set, Timeouts wins.
    // This is a real migration ordering — pin it so the migration semantics are explicit.
    NettyRequestReplySpec.Timeouts nested = new NettyRequestReplySpec.Timeouts();
    nested.setCallTimeout(Duration.ofMinutes(7));
    nested.setConnectTimeout(Duration.ofSeconds(11));

    NettyRequestReplySpec spec =
        new NettyRequestReplySpec(
            Duration.ofMinutes(99),
            Duration.ofSeconds(99),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            nested);

    assertThat(spec.callTimeout).isEqualTo(Duration.ofMinutes(7));
    assertThat(spec.connectTimeout).isEqualTo(Duration.ofSeconds(11));
  }

  @Test
  void mtlsCertFieldsArePreservedWhenProvided() {
    NettyRequestReplySpec spec =
        new NettyRequestReplySpec(
            null,
            null,
            null,
            null,
            null,
            "ca.pem",
            "client.pem",
            "client.key",
            "secret-password",
            null);

    assertThat(spec.getTrustedCaCerts()).hasValue("ca.pem");
    assertThat(spec.getClientCerts()).hasValue("client.pem");
    assertThat(spec.getClientKey()).hasValue("client.key");
    assertThat(spec.getClientKeyPassword()).hasValue("secret-password");
  }

  @Test
  void timeoutsObjectRejectsZeroCallTimeout() {
    NettyRequestReplySpec.Timeouts t = new NettyRequestReplySpec.Timeouts();
    assertThatThrownBy(() -> t.setCallTimeout(Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Timeout durations must be larger than 0");
  }

  @Test
  void timeoutsObjectRejectsZeroConnectTimeout() {
    NettyRequestReplySpec.Timeouts t = new NettyRequestReplySpec.Timeouts();
    assertThatThrownBy(() -> t.setConnectTimeout(Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void timeoutsObjectRejectsNullDurations() {
    NettyRequestReplySpec.Timeouts t = new NettyRequestReplySpec.Timeouts();
    assertThatThrownBy(() -> t.setCallTimeout(null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> t.setConnectTimeout(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void deserializesEmptyJsonAsAllDefaults() throws Exception {
    NettyRequestReplySpec spec = mapper.readValue("{}", NettyRequestReplySpec.class);

    assertThat(spec.callTimeout).isEqualTo(NettyRequestReplySpec.DEFAULT_CALL_TIMEOUT);
    assertThat(spec.connectionPoolMaxSize)
        .isEqualTo(NettyRequestReplySpec.DEFAULT_CONNECTION_POOL_MAX_SIZE);
  }

  @Test
  void deserializesPropertyNamesUsingTheJsonPropertyConstants() throws Exception {
    String json =
        "{\""
            + NettyRequestReplySpec.CONNECTION_POOL_MAX_SIZE_PROPERTY
            + "\": 64,\""
            + NettyRequestReplySpec.MAX_REQUEST_OR_RESPONSE_SIZE_IN_BYTES_PROPERTY
            + "\": 1024}";

    NettyRequestReplySpec spec = mapper.readValue(json, NettyRequestReplySpec.class);

    assertThat(spec.connectionPoolMaxSize).isEqualTo(64);
    assertThat(spec.maxRequestOrResponseSizeInBytes).isEqualTo(1024);
  }

  @Test
  void timeoutsDefaultsAreApplicableWhenNoSetterCalled() {
    NettyRequestReplySpec.Timeouts t = new NettyRequestReplySpec.Timeouts();
    // No setters called — pin the default values exposed through the getter API.
    assertThat(t.getCallTimeout()).isEqualTo(Duration.ofMinutes(1));
    assertThat(t.getConnectTimeout()).isEqualTo(Duration.ofSeconds(10));
  }
}
