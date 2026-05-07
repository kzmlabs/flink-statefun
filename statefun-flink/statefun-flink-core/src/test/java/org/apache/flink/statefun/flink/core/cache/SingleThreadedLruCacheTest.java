// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.cache;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

public class SingleThreadedLruCacheTest {

  @Test
  public void exampleUsage() {
    SingleThreadedLruCache<String, String> cache = new SingleThreadedLruCache<>(2);

    cache.put("a", "1");
    cache.put("b", "2");

    assertThat(cache.get("a"), is("1"));
    assertThat(cache.get("b"), is("2"));
  }

  @Test
  public void leastRecentlyElementShouldBeEvicted() {
    SingleThreadedLruCache<String, String> cache = new SingleThreadedLruCache<>(2);

    cache.put("a", "1");
    cache.put("b", "2");
    cache.put("c", "3");

    assertThat(cache.get("a"), nullValue());
    assertThat(cache.get("b"), is("2"));
    assertThat(cache.get("c"), is("3"));
  }

  @Test
  public void touchedEntryIsKeptOverUntouchedDuringEviction() {
    SingleThreadedLruCache<String, String> cache = new SingleThreadedLruCache<>(2);
    cache.put("a", "1");
    cache.put("b", "2");

    // Touch a so b becomes the LRU; inserting c should evict b.
    cache.get("a");
    cache.put("c", "3");

    assertThat(cache.get("a"), is("1"));
    assertThat(cache.get("b"), nullValue());
    assertThat(cache.get("c"), is("3"));
  }

  @Test
  public void putOverwritesExistingEntry() {
    SingleThreadedLruCache<String, String> cache = new SingleThreadedLruCache<>(2);
    cache.put("a", "1");
    cache.put("a", "2");

    assertThat(cache.get("a"), is("2"));
  }

  @Test
  public void computeIfAbsentInvokesMapperOnlyOnFirstAccess() {
    SingleThreadedLruCache<String, Integer> cache = new SingleThreadedLruCache<>(4);
    AtomicInteger callCount = new AtomicInteger();

    Integer first =
        cache.computeIfAbsent(
            "k",
            key -> {
              callCount.incrementAndGet();
              return 99;
            });
    Integer second =
        cache.computeIfAbsent(
            "k",
            key -> {
              callCount.incrementAndGet();
              return 77;
            });

    assertThat(first, is(99));
    assertThat(second, is(99));
    assertThat(callCount.get(), is(1));
  }
}
