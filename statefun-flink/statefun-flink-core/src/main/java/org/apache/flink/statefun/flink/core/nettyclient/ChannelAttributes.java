// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.nettyclient;

import org.apache.flink.shaded.netty4.io.netty.util.AttributeKey;

final class ChannelAttributes {

  private ChannelAttributes() {}

  static final AttributeKey<Boolean> EXPIRED =
      AttributeKey.valueOf("org.apache.flink.statefun.flink.core.nettyclient.ExpiredKey");
  static final AttributeKey<Boolean> ACQUIRED =
      AttributeKey.valueOf("org.apache.flink.statefun.flink.core.nettyclient.AcquiredKey");
}
