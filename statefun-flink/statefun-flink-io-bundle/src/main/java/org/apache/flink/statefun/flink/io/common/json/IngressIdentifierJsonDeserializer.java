// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.io.common.json;

import com.google.protobuf.Message;
import java.io.IOException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;

public final class IngressIdentifierJsonDeserializer
    extends JsonDeserializer<IngressIdentifier<Message>> {
  @Override
  public IngressIdentifier<Message> deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    final TypeName id = TypeName.parseFrom(jsonParser.getText());
    return new IngressIdentifier<>(Message.class, id.namespace(), id.name());
  }
}
