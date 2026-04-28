// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.java.types;

import org.apache.flink.statefun.sdk.java.slice.Slice;

public interface TypeSerializer<T> {

  Slice serialize(T value);

  T deserialize(Slice bytes);
}
