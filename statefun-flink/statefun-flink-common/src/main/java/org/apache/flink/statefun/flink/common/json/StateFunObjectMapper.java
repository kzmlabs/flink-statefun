// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.common.json;

import java.io.IOException;
import java.time.Duration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.util.TimeUtils;

public final class StateFunObjectMapper {

  public static ObjectMapper create() {
    final ObjectMapper mapper =
        new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    final SimpleModule module = new SimpleModule("statefun");
    module.addSerializer(Duration.class, new DurationJsonSerializer());
    module.addDeserializer(Duration.class, new DurationJsonDeserializer());
    module.addDeserializer(TypeName.class, new TypeNameJsonDeserializer());

    mapper.registerModule(module);
    return mapper;
  }

  private static final class DurationJsonDeserializer extends JsonDeserializer<Duration> {
    @Override
    public Duration deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      return TimeUtils.parseDuration(jsonParser.getText());
    }
  }

  private static final class DurationJsonSerializer extends JsonSerializer<Duration> {
    @Override
    public void serialize(
        Duration duration, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException {
      jsonGenerator.writeString(TimeUtils.formatWithHighestUnit(duration));
    }
  }

  private static final class TypeNameJsonDeserializer extends JsonDeserializer<TypeName> {
    @Override
    public TypeName deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      return TypeName.parseFrom(jsonParser.getText());
    }
  }
}
