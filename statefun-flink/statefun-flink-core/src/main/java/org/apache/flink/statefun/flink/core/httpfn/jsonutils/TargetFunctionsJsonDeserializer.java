// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.httpfn.jsonutils;

import java.io.IOException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.statefun.flink.core.httpfn.TargetFunctions;

public final class TargetFunctionsJsonDeserializer extends JsonDeserializer<TargetFunctions> {
  @Override
  public TargetFunctions deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    return TargetFunctions.fromPatternString(jsonParser.getText());
  }
}
