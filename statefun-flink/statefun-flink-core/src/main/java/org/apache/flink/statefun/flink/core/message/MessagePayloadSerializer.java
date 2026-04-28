// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.message;

import javax.annotation.Nonnull;
import org.apache.flink.statefun.flink.core.generated.Payload;

public interface MessagePayloadSerializer {

  Payload serialize(@Nonnull Object payloadObject);

  Object deserialize(@Nonnull ClassLoader targetClassLoader, @Nonnull Payload payload);

  Object copy(@Nonnull ClassLoader targetClassLoader, @Nonnull Object what);
}
