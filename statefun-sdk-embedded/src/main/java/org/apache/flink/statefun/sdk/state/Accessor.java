// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.state;

public interface Accessor<T> {

  void set(T value);

  T get();

  void clear();
}
