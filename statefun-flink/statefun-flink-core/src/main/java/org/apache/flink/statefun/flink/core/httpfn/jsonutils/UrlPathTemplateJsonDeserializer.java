// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.httpfn.jsonutils;

import java.io.IOException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.statefun.flink.core.httpfn.UrlPathTemplate;

public final class UrlPathTemplateJsonDeserializer extends JsonDeserializer<UrlPathTemplate> {
  @Override
  public UrlPathTemplate deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    return new UrlPathTemplate(jsonParser.getText());
  }
}
