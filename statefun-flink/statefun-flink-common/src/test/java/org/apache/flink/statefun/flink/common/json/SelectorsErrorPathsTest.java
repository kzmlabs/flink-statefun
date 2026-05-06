// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.common.json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

/** Covers Selectors error-paths and missing-key behaviour not exercised by SelectorsTest. */
class SelectorsErrorPathsTest {

  private static final JsonPointer FOO = JsonPointer.valueOf("/foo");
  private final ObjectMapper mapper = new ObjectMapper();

  // -------- textAt --------

  @Test
  void textAtOnNonStringThrowsWrongType() {
    ObjectNode node = mapper.createObjectNode();
    node.put("foo", 42);

    assertThatThrownBy(() -> Selectors.textAt(node, FOO)).isInstanceOf(WrongTypeException.class);
  }

  @Test
  void textAtOnMissingKeyThrowsMissingKey() {
    ObjectNode node = mapper.createObjectNode();

    assertThatThrownBy(() -> Selectors.textAt(node, FOO)).isInstanceOf(MissingKeyException.class);
  }

  @Test
  void optionalTextAtOnNonStringThrowsWrongType() {
    ObjectNode node = mapper.createObjectNode();
    node.put("foo", 42);

    assertThatThrownBy(() -> Selectors.optionalTextAt(node, FOO))
        .isInstanceOf(WrongTypeException.class);
  }

  // -------- integerAt / longAt --------

  @Test
  void integerAtOnTextThrowsWrongType() {
    ObjectNode node = mapper.createObjectNode();
    node.put("foo", "not-an-int");

    assertThatThrownBy(() -> Selectors.integerAt(node, FOO))
        .isInstanceOf(WrongTypeException.class);
  }

  @Test
  void longAtAcceptsBothLongAndInt() {
    ObjectNode node = mapper.createObjectNode();
    node.put("intval", 42);
    node.put("longval", 1234567890123L);

    assertThat(Selectors.longAt(node, JsonPointer.valueOf("/intval"))).isEqualTo(42L);
    assertThat(Selectors.longAt(node, JsonPointer.valueOf("/longval"))).isEqualTo(1234567890123L);
  }

  @Test
  void longAtOnTextThrowsWrongType() {
    ObjectNode node = mapper.createObjectNode();
    node.put("foo", "abc");

    assertThatThrownBy(() -> Selectors.longAt(node, FOO))
        .isInstanceOf(WrongTypeException.class);
  }

  @Test
  void optionalLongAtOnMissingKeyReturnsEmpty() {
    ObjectNode node = mapper.createObjectNode();

    assertThat(Selectors.optionalLongAt(node, FOO).isEmpty()).isTrue();
  }

  @Test
  void optionalLongAtOnTextThrowsWrongType() {
    ObjectNode node = mapper.createObjectNode();
    node.put("foo", "abc");

    assertThatThrownBy(() -> Selectors.optionalLongAt(node, FOO))
        .isInstanceOf(WrongTypeException.class);
  }

  @Test
  void optionalIntegerAtOnMissingKeyReturnsEmpty() {
    ObjectNode node = mapper.createObjectNode();

    assertThat(Selectors.optionalIntegerAt(node, FOO).isEmpty()).isTrue();
  }

  @Test
  void optionalIntegerAtOnNonIntegerThrowsWrongType() {
    ObjectNode node = mapper.createObjectNode();
    node.put("foo", "abc");

    assertThatThrownBy(() -> Selectors.optionalIntegerAt(node, FOO))
        .isInstanceOf(WrongTypeException.class);
  }

  @Test
  void optionalIntegerAtOnLongValueThrowsWrongType() {
    // optionalIntegerAt accepts only Int (not Long); pin that vs longAt's tolerance.
    ObjectNode node = mapper.createObjectNode();
    node.put("foo", 1234567890123L);

    assertThatThrownBy(() -> Selectors.optionalIntegerAt(node, FOO))
        .isInstanceOf(WrongTypeException.class);
  }

  // -------- durationAt --------

  @Test
  void durationAtOnNonStringThrowsWrongType() {
    ObjectNode node = mapper.createObjectNode();
    node.put("foo", 30);

    assertThatThrownBy(() -> Selectors.durationAt(node, FOO))
        .isInstanceOf(WrongTypeException.class);
  }

  @Test
  void durationAtOnInvalidDurationStringThrowsWrongType() {
    ObjectNode node = mapper.createObjectNode();
    node.put("foo", "not-a-duration");

    // Pin: the implementation catches IllegalArgumentException from TimeUtils and re-wraps
    // as WrongTypeException for consistent error semantics at the Selectors layer.
    assertThatThrownBy(() -> Selectors.durationAt(node, FOO))
        .isInstanceOf(WrongTypeException.class);
  }

  @Test
  void optionalDurationAtOnMissingKeyReturnsEmpty() {
    ObjectNode node = mapper.createObjectNode();

    assertThat(Selectors.optionalDurationAt(node, FOO).isEmpty()).isTrue();
  }

  @Test
  void optionalDurationAtOnInvalidStringThrowsWrongType() {
    ObjectNode node = mapper.createObjectNode();
    node.put("foo", "not-a-duration");

    assertThatThrownBy(() -> Selectors.optionalDurationAt(node, FOO))
        .isInstanceOf(WrongTypeException.class);
  }

  @Test
  void optionalDurationAtOnNonStringThrowsWrongType() {
    ObjectNode node = mapper.createObjectNode();
    node.put("foo", 42);

    assertThatThrownBy(() -> Selectors.optionalDurationAt(node, FOO))
        .isInstanceOf(WrongTypeException.class);
  }

  // -------- listAt / textListAt / propertiesAt --------

  @Test
  void listAtOnMissingKeyReturnsEmptyIterable() {
    ObjectNode node = mapper.createObjectNode();

    Iterable<? extends JsonNode> result = Selectors.listAt(node, FOO);

    assertThat(result.iterator().hasNext()).isFalse();
  }

  @Test
  void listAtOnNonArrayThrowsWrongType() {
    ObjectNode node = mapper.createObjectNode();
    node.put("foo", "not-a-list");

    assertThatThrownBy(() -> Selectors.listAt(node, FOO))
        .isInstanceOf(WrongTypeException.class);
  }

  @Test
  void textListAtOnMissingKeyReturnsEmptyList() {
    ObjectNode node = mapper.createObjectNode();

    assertThat(Selectors.textListAt(node, FOO)).isEmpty();
  }

  @Test
  void textListAtOnNonArrayThrowsWrongType() {
    ObjectNode node = mapper.createObjectNode();
    node.put("foo", "not-a-list");

    assertThatThrownBy(() -> Selectors.textListAt(node, FOO))
        .isInstanceOf(WrongTypeException.class);
  }

  @Test
  void propertiesAtOnMissingKeyReturnsEmptyMap() {
    ObjectNode node = mapper.createObjectNode();

    assertThat(Selectors.propertiesAt(node, FOO)).isEmpty();
  }

  @Test
  void propertiesAtOnNonArrayThrowsWrongType() {
    ObjectNode node = mapper.createObjectNode();
    node.put("foo", "not-a-list");

    assertThatThrownBy(() -> Selectors.propertiesAt(node, FOO))
        .isInstanceOf(WrongTypeException.class);
  }

  @Test
  void longPropertiesAtRejectsNonLongValueWithDescriptiveMessage() {
    ObjectNode node = mapper.createObjectNode();
    org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode arr =
        mapper.createArrayNode();
    ObjectNode entry = mapper.createObjectNode();
    entry.put("k", "not-a-long");
    arr.add(entry);
    node.set("foo", arr);

    assertThatThrownBy(() -> Selectors.longPropertiesAt(node, FOO))
        .isInstanceOf(WrongTypeException.class)
        .hasMessageContaining("k");
  }

  // -------- optionalObjectAt --------

  @Test
  void optionalObjectAtOnMissingKeyReturnsEmpty() {
    ObjectNode node = mapper.createObjectNode();

    Optional<ObjectNode> result = Selectors.optionalObjectAt(node, FOO);

    assertThat(result).isEmpty();
  }

  @Test
  void optionalObjectAtOnNonObjectThrowsWrongType() {
    ObjectNode node = mapper.createObjectNode();
    node.put("foo", "string-not-object");

    assertThatThrownBy(() -> Selectors.optionalObjectAt(node, FOO))
        .isInstanceOf(WrongTypeException.class);
  }

  @Test
  void optionalObjectAtOnObjectReturnsIt() {
    ObjectNode node = mapper.createObjectNode();
    ObjectNode child = mapper.createObjectNode();
    child.put("k", "v");
    node.set("foo", child);

    Optional<ObjectNode> result = Selectors.optionalObjectAt(node, FOO);

    assertThat(result).isPresent();
    assertThat(result.get().get("k").asText()).isEqualTo("v");
  }
}
