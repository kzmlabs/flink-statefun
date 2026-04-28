// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.cache;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

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
}
