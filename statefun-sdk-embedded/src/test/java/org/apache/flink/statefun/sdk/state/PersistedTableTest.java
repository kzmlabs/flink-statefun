// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.sdk.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Edge-case coverage for {@link PersistedTable}.
 *
 * <p>Contract note: {@code PersistedTable} is single-threaded by Flink design (per-key state is
 * processed one message at a time). The default {@link PersistedTable.NonFaultTolerantAccessor} is
 * backed by an unsynchronized {@link java.util.HashMap}; nothing in the source or javadoc claims
 * thread-safety. Tests therefore assert the single-threaded contract: fail-fast on
 * mid-iteration mutation, deterministic null-key/null-value behavior inherited from {@code
 * HashMap}, idempotent removal, and clean clear semantics.
 */
public class PersistedTableTest {

  private PersistedTable<String, Integer> table;

  @BeforeEach
  public void setUp() {
    table = PersistedTable.of("test-table", String.class, Integer.class);
  }

  @Test
  public void getOnEmptyTableReturnsNull() {
    assertThat(table.get("missing")).isNull();
  }

  @Test
  public void setThenGetRoundtrip() {
    table.set("key", 42);

    assertThat(table.get("key")).isEqualTo(42);
  }

  @Test
  public void setOverwritesPreviousValue() {
    table.set("key", 1);
    table.set("key", 2);

    assertThat(table.get("key")).isEqualTo(2);
  }

  /**
   * The default accessor delegates to {@link java.util.HashMap}, which permits null keys. This test
   * documents that today a null key is silently accepted rather than rejected with a clear error.
   *
   * <p>TODO(#149): validate {@code key} at the {@link PersistedTable#set(Object, Object)} boundary
   * so misuse fails loudly with a parameter-named NPE instead of corrupting the keyspace with a
   * silent {@code null} entry.
   */
  @Test
  public void nullKeyIsSilentlyAcceptedToday() {
    assertThatCode(() -> table.set(null, 7)).doesNotThrowAnyException();
    assertThat(table.get(null)).isEqualTo(7);
    assertThat(table.keys()).containsExactly((String) null);
  }

  /**
   * The default accessor allows null values (HashMap does). Set followed by get returns null, which
   * is indistinguishable from "absent" — callers must use {@link PersistedTable#keys()} or {@link
   * PersistedTable#entries()} to disambiguate.
   */
  @Test
  public void nullValueIsAcceptedAndReadBackAsNull() {
    table.set("key", null);

    assertThat(table.get("key")).isNull();
    assertThat(table.keys()).containsExactly("key");
  }

  @Test
  public void removeOfMissingKeyDoesNotThrowOrCorruptState() {
    table.set("present", 1);

    assertThatCode(() -> table.remove("absent")).doesNotThrowAnyException();

    assertThat(table.get("present")).isEqualTo(1);
    assertThat(table.get("absent")).isNull();
    assertThat(table.keys()).containsExactly("present");
  }

  @Test
  public void removeDeletesExistingEntry() {
    table.set("key", 99);
    table.remove("key");

    assertThat(table.get("key")).isNull();
    assertThat(table.keys()).isEmpty();
  }

  @Test
  public void clearResetsToEmpty() {
    table.set("a", 1);
    table.set("b", 2);
    table.set("c", 3);

    table.clear();

    assertThat(table.entries()).isEmpty();
    assertThat(table.keys()).isEmpty();
    assertThat(table.values()).isEmpty();
    assertThat(table.get("a")).isNull();
  }

  @Test
  public void clearedTableAcceptsNewEntries() {
    table.set("a", 1);
    table.clear();
    table.set("a", 2);

    assertThat(table.get("a")).isEqualTo(2);
  }

  /**
   * Verifies the fail-fast contract: the underlying {@link java.util.HashMap} throws {@link
   * ConcurrentModificationException} when the table is mutated during iteration. This is the
   * single-threaded misuse signal — silent corruption would be far worse.
   */
  @Test
  public void mutationDuringIterationFailsFast() {
    table.set("a", 1);
    table.set("b", 2);

    Iterator<Map.Entry<String, Integer>> iterator = table.entries().iterator();
    iterator.next();

    table.set("c", 3);

    assertThatThrownBy(iterator::next).isInstanceOf(ConcurrentModificationException.class);
  }

  @Test
  public void removeDuringIterationFailsFast() {
    table.set("a", 1);
    table.set("b", 2);

    Iterator<String> iterator = table.keys().iterator();
    iterator.next();

    table.remove("b");

    assertThatThrownBy(iterator::next).isInstanceOf(ConcurrentModificationException.class);
  }

  @Test
  public void largeKeyPayloadRoundtrip() {
    String largeKey = "k".repeat(64 * 1024);

    table.set(largeKey, 123);

    assertThat(table.get(largeKey)).isEqualTo(123).isNotNull();
  }

  @Test
  public void entriesReflectAllInsertions() {
    table.set("a", 1);
    table.set("b", 2);
    table.set("c", 3);

    assertThat(table.keys()).containsExactlyInAnyOrder("a", "b", "c");
    assertThat(table.values()).containsExactlyInAnyOrder(1, 2, 3);
  }

  /**
   * Concurrent multi-threaded access is not part of the {@link PersistedTable} contract. Flink
   * routes one message per key at a time, so user functions never see the table from two threads.
   * Documented here as an explicit skip rather than as a flaky load test that asserts wishes the
   * code does not promise.
   */
  @Test
  public void concurrentAccessIsOutsideContract() {
    assumeTrue(
        false,
        "PersistedTable is single-threaded by Flink design — concurrent-access behavior is undefined.");
  }
}
