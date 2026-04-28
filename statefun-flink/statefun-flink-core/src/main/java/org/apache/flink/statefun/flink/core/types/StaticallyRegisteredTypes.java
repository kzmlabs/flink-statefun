// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.types;

import com.google.protobuf.Message;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.statefun.flink.common.protobuf.ProtobufTypeInformation;
import org.apache.flink.statefun.flink.core.message.MessageFactoryKey;
import org.apache.flink.statefun.flink.core.message.MessageTypeInformation;

/**
 * StaticallyRegisteredTypes are types that were registered during the creation of the Stateful
 * Functions universe.
 */
@NotThreadSafe
@SuppressWarnings("unchecked")
public final class StaticallyRegisteredTypes {

  private final Map<Class<?>, TypeInformation<?>> registeredTypes = new HashMap<>();

  public StaticallyRegisteredTypes(MessageFactoryKey messageFactoryKey) {
    this.messageFactoryKey = messageFactoryKey;
  }

  private final MessageFactoryKey messageFactoryKey;

  public <T> TypeInformation<T> registerType(Class<T> type) {
    return (TypeInformation<T>) registeredTypes.computeIfAbsent(type, this::typeInformation);
  }

  /**
   * Retrieves the previously registered type. This is safe to access concurrently, after the
   * translation phase is over.
   */
  @Nullable
  <T> TypeInformation<T> getType(Class<T> valueType) {
    return (TypeInformation<T>) registeredTypes.get(valueType);
  }

  private TypeInformation<?> typeInformation(Class<?> valueType) {
    if (Message.class.isAssignableFrom(valueType)) {
      Class<Message> message = (Class<Message>) valueType;
      return new ProtobufTypeInformation<>(message);
    }
    if (org.apache.flink.statefun.flink.core.message.Message.class.isAssignableFrom(valueType)) {
      return new MessageTypeInformation(messageFactoryKey);
    }
    // TODO: we may want to restrict the allowed typeInfo here to theses that respect shcema
    // evaluation.
    return TypeInformation.of(valueType);
  }
}
