// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.httpfn.jsonutils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.statefun.flink.core.httpfn.TargetFunctions;
import org.apache.flink.statefun.flink.core.httpfn.UrlPathTemplate;
import org.apache.flink.statefun.sdk.FunctionType;
import org.junit.jupiter.api.Test;

class HttpfnJsonDeserializersTest {

  private final ObjectMapper mapper = mapperWith();

  @Test
  void targetFunctionsDeserializerParsesNamespaceNamePatternToSpecificType() throws Exception {
    TargetFunctionsHolder holder =
        mapper.readValue("{\"value\":\"counter/inc\"}", TargetFunctionsHolder.class);

    assertThat(holder.value.isSpecificFunctionType()).isTrue();
    assertThat(holder.value.asSpecificFunctionType())
        .isEqualTo(new FunctionType("counter", "inc"));
  }

  @Test
  void targetFunctionsDeserializerParsesNamespaceWildcardToNamespaceMatcher() throws Exception {
    TargetFunctionsHolder holder =
        mapper.readValue("{\"value\":\"counter/*\"}", TargetFunctionsHolder.class);

    assertThat(holder.value.isNamespace()).isTrue();
    assertThat(holder.value.asNamespace().targetNamespace()).isEqualTo("counter");
  }

  @Test
  void targetFunctionsDeserializerSurfacesInvalidPatternsAsCleanError() {
    // Invalid pattern (commas) — production parser throws IllegalArgumentException; Jackson
    // wraps as JsonMappingException. Either is acceptable so long as the failure is loud.
    assertThatThrownBy(
            () ->
                mapper.readValue(
                    "{\"value\":\"counter/a, counter/b\"}", TargetFunctionsHolder.class))
        .satisfiesAnyOf(
            t -> assertThat(t).isInstanceOf(IllegalArgumentException.class),
            t -> assertThat(t).isInstanceOf(JsonMappingException.class));
  }

  @Test
  void urlPathTemplateDeserializerProducesSubstitutableTemplate() throws Exception {
    UrlPathTemplateHolder holder =
        mapper.readValue(
            "{\"value\":\"http://upstream/api/{function.name}\"}", UrlPathTemplateHolder.class);

    URI applied = holder.value.apply(new FunctionType("counter", "increment"));

    assertThat(applied.toString()).isEqualTo("http://upstream/api/increment");
  }

  @Test
  void urlPathTemplateDeserializerHandlesTemplateWithoutPlaceholder() throws Exception {
    UrlPathTemplateHolder holder =
        mapper.readValue("{\"value\":\"http://static-endpoint\"}", UrlPathTemplateHolder.class);

    URI applied = holder.value.apply(new FunctionType("any", "any"));

    assertThat(applied.toString()).isEqualTo("http://static-endpoint");
  }

  private static ObjectMapper mapperWith() {
    ObjectMapper m = new ObjectMapper();
    SimpleModule mod = new SimpleModule();
    mod.addDeserializer(TargetFunctions.class, new TargetFunctionsJsonDeserializer());
    mod.addDeserializer(UrlPathTemplate.class, new UrlPathTemplateJsonDeserializer());
    m.registerModule(mod);
    return m;
  }

  static final class TargetFunctionsHolder {
    public TargetFunctions value;

    public TargetFunctionsHolder() {}
  }

  static final class UrlPathTemplateHolder {
    public UrlPathTemplate value;

    public UrlPathTemplateHolder() {}
  }
}
