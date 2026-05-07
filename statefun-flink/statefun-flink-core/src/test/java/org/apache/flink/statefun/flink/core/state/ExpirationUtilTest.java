// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.StateTtlConfig.UpdateType;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.statefun.sdk.state.Expiration;
import org.junit.jupiter.api.Test;

class ExpirationUtilTest {

  @Test
  void noneExpirationLeavesTtlDisabled() {
    ValueStateDescriptor<String> handle =
        new ValueStateDescriptor<>("v", BasicTypeInfo.STRING_TYPE_INFO);

    ExpirationUtil.configureStateTtl(handle, Expiration.none());

    assertThat(handle.getTtlConfig().isEnabled()).isFalse();
  }

  @Test
  void afterWriteExpirationEnablesOnCreateAndWriteUpdateType() {
    ValueStateDescriptor<String> handle =
        new ValueStateDescriptor<>("v", BasicTypeInfo.STRING_TYPE_INFO);

    ExpirationUtil.configureStateTtl(handle, Expiration.expireAfterWriting(Duration.ofMinutes(5)));

    StateTtlConfig ttl = handle.getTtlConfig();
    assertThat(ttl.isEnabled()).isTrue();
    assertThat(ttl.getUpdateType()).isEqualTo(UpdateType.OnCreateAndWrite);
    assertThat(ttl.getTimeToLive()).isEqualTo(java.time.Duration.ofMinutes(5));
  }

  @Test
  void afterReadOrWriteExpirationEnablesOnReadAndWriteUpdateType() {
    ValueStateDescriptor<String> handle =
        new ValueStateDescriptor<>("v", BasicTypeInfo.STRING_TYPE_INFO);

    ExpirationUtil.configureStateTtl(
        handle, Expiration.expireAfterReadingOrWriting(Duration.ofSeconds(30)));

    StateTtlConfig ttl = handle.getTtlConfig();
    assertThat(ttl.isEnabled()).isTrue();
    assertThat(ttl.getUpdateType()).isEqualTo(UpdateType.OnReadAndWrite);
    assertThat(ttl.getTimeToLive()).isEqualTo(java.time.Duration.ofSeconds(30));
  }

  @Test
  void neverReturnExpiredVisibilityIsAlwaysSet() {
    // Pin invariant: regardless of mode, never-return-expired visibility is the only
    // safe choice for StateFun semantics — leaking expired values would surface stale
    // function state.
    ValueStateDescriptor<String> handle =
        new ValueStateDescriptor<>("v", BasicTypeInfo.STRING_TYPE_INFO);

    ExpirationUtil.configureStateTtl(handle, Expiration.expireAfterWriting(Duration.ofSeconds(1)));

    assertThat(handle.getTtlConfig().getStateVisibility())
        .isEqualTo(StateTtlConfig.StateVisibility.NeverReturnExpired);
  }

  @Test
  void afterWriteWithSubMillisecondDurationIsRoundedToMillisecondPrecision() {
    // ExpirationUtil normalizes via Duration.toMillis() — sub-millisecond input is
    // truncated to 0 by Flink's StateTtlConfig.Builder which then rejects 0.
    ValueStateDescriptor<String> handle =
        new ValueStateDescriptor<>("v", BasicTypeInfo.STRING_TYPE_INFO);

    assertThatThrownBy(
            () ->
                ExpirationUtil.configureStateTtl(
                    handle, Expiration.expireAfterWriting(Duration.ofNanos(500))))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
