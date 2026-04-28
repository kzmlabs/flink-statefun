// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.state;

import java.time.Duration;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.StateTtlConfig.Builder;
import org.apache.flink.api.common.state.StateTtlConfig.StateVisibility;
import org.apache.flink.api.common.state.StateTtlConfig.TtlTimeCharacteristic;
import org.apache.flink.api.common.state.StateTtlConfig.UpdateType;
import org.apache.flink.statefun.sdk.state.Expiration;
import org.apache.flink.statefun.sdk.state.Expiration.Mode;

final class ExpirationUtil {
  private ExpirationUtil() {}

  static void configureStateTtl(StateDescriptor<?, ?> handle, Expiration expiration) {
    if (expiration.mode() == Mode.NONE) {
      return;
    }
    StateTtlConfig ttlConfig = from(expiration);
    handle.enableTimeToLive(ttlConfig);
  }

  private static StateTtlConfig from(Expiration expiration) {
    final long millis = expiration.duration().toMillis();
    Builder builder = StateTtlConfig.newBuilder(Duration.ofMillis(millis));
    builder.setTtlTimeCharacteristic(TtlTimeCharacteristic.ProcessingTime);
    builder.setStateVisibility(StateVisibility.NeverReturnExpired);
    switch (expiration.mode()) {
      case AFTER_WRITE:
        {
          builder.setUpdateType(UpdateType.OnCreateAndWrite);
          break;
        }
      case AFTER_READ_OR_WRITE:
        {
          builder.setUpdateType(UpdateType.OnReadAndWrite);
          break;
        }
      default:
        throw new IllegalArgumentException("Unknown expiration mode " + expiration.mode());
    }
    return builder.build();
  }
}
