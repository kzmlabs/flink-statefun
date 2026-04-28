// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.state;

public class ApiExtension {
  public static <T> void setPersistedValueAccessor(
      PersistedValue<T> persistedValue, Accessor<T> accessor) {
    persistedValue.setAccessor(accessor);
  }

  public static <K, V> void setPersistedTableAccessor(
      PersistedTable<K, V> persistedTable, TableAccessor<K, V> accessor) {
    persistedTable.setAccessor(accessor);
  }

  public static <E> void setPersistedAppendingBufferAccessor(
      PersistedAppendingBuffer<E> persistedAppendingBuffer, AppendingBufferAccessor<E> accessor) {
    persistedAppendingBuffer.setAccessor(accessor);
  }

  public static void setRemotePersistedValueAccessor(
      RemotePersistedValue remotePersistedValue, Accessor<byte[]> accessor) {
    remotePersistedValue.setAccessor(accessor);
  }

  public static void bindPersistedStateRegistry(
      PersistedStateRegistry persistedStateRegistry, StateBinder stateBinder) {
    persistedStateRegistry.bind(stateBinder);
  }
}
