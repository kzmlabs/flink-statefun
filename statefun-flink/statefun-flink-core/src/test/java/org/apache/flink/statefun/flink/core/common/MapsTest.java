// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MapsTest {

  @Test
  void transformValuesAppliesFunctionToEveryValue() {
    Map<String, Integer> input = new HashMap<>();
    input.put("a", 1);
    input.put("b", 2);

    Map<String, String> result = Maps.transformValues(input, v -> "v=" + v);

    assertThat(result).containsExactlyInAnyOrderEntriesOf(Map.of("a", "v=1", "b", "v=2"));
  }

  @Test
  void transformValuesOnEmptyMapReturnsEmpty() {
    Map<String, String> result =
        Maps.transformValues(Collections.<String, String>emptyMap(), v -> v);

    assertThat(result).isEmpty();
  }

  @Test
  void transformKeysAppliesFunctionToEveryKey() {
    Map<Integer, String> input = new HashMap<>();
    input.put(1, "a");
    input.put(2, "b");

    Map<String, String> result = Maps.transformKeys(input, k -> "k=" + k);

    assertThat(result).containsExactlyInAnyOrderEntriesOf(Map.of("k=1", "a", "k=2", "b"));
  }

  @Test
  void transformValuesBiFunctionReceivesKeyAndValue() {
    Map<String, Integer> input = new HashMap<>();
    input.put("a", 1);
    input.put("b", 2);

    Map<String, String> result = Maps.transformValues(input, (k, v) -> k + "=" + v);

    assertThat(result).contains(entry("a", "a=1"), entry("b", "b=2"));
  }

  @Test
  void indexBuildsKeyedMapFromIterable() {
    List<String> elements = Arrays.asList("alpha", "beta", "gamma");

    Map<Character, String> indexed = Maps.index(elements, s -> s.charAt(0));

    assertThat(indexed).containsExactlyInAnyOrderEntriesOf(Map.of('a', "alpha", 'b', "beta", 'g', "gamma"));
  }

  @Test
  void indexCollidingKeysOverwriteEarlierEntries() {
    // Pin: when two elements map to the same key, the LAST insertion wins (HashMap.put).
    // Callers relying on this for de-duplication should know the order they iterate matters.
    List<String> elements = Arrays.asList("apple", "ant", "banana");

    Map<Character, String> indexed = Maps.index(elements, s -> s.charAt(0));

    assertThat(indexed).hasSize(2).containsEntry('a', "ant").containsEntry('b', "banana");
  }
}
