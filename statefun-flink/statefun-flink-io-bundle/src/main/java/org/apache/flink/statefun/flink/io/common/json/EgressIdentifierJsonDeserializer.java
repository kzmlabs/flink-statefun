// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.io.common.json;

import java.io.IOException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public final class EgressIdentifierJsonDeserializer
    extends JsonDeserializer<EgressIdentifier<TypedValue>> {
  @Override
  public EgressIdentifier<TypedValue> deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    final TypeName id = TypeName.parseFrom(jsonParser.getText());
    return new EgressIdentifier<>(id.namespace(), id.name(), TypedValue.class);
  }
}
