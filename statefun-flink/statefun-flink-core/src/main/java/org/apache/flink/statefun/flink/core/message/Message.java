// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.message;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.flink.core.memory.DataOutputView;

public interface Message extends RoutableMessage {

  Object payload(MessageFactory context, ClassLoader targetClassLoader);

  /**
   * isBarrierMessage - returns an empty optional for non barrier messages or wrapped checkpointId
   * for barrier messages.
   *
   * <p>When this message represents a checkpoint barrier, this method returns an {@code Optional}
   * of a checkpoint id that produced that barrier. For other types of messages (i.e. {@code
   * Payload}) this method returns an empty {@code Optional}.
   */
  OptionalLong isBarrierMessage();

  Optional<String> cancellationToken();

  Message copy(MessageFactory context);

  void writeTo(MessageFactory context, DataOutputView target) throws IOException;

  default void postApply() {}
}
