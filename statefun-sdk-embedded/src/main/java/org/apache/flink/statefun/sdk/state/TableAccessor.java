// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.state;

import java.util.Map;

public interface TableAccessor<K, V> {

  void set(K key, V value);

  V get(K key);

  void remove(K key);

  Iterable<Map.Entry<K, V>> entries();

  Iterable<K> keys();

  Iterable<V> values();

  void clear();
}
