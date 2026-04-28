// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.state;

import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.state.Accessor;
import org.apache.flink.statefun.sdk.state.AppendingBufferAccessor;
import org.apache.flink.statefun.sdk.state.PersistedAppendingBuffer;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import org.apache.flink.statefun.sdk.state.RemotePersistedValue;
import org.apache.flink.statefun.sdk.state.TableAccessor;

public interface State {

  <T> Accessor<T> createFlinkStateAccessor(
      FunctionType functionType, PersistedValue<T> persistedValue);

  <K, V> TableAccessor<K, V> createFlinkStateTableAccessor(
      FunctionType functionType, PersistedTable<K, V> persistedTable);

  <E> AppendingBufferAccessor<E> createFlinkStateAppendingBufferAccessor(
      FunctionType functionType, PersistedAppendingBuffer<E> persistedAppendingBuffer);

  Accessor<byte[]> createFlinkRemoteStateAccessor(
      FunctionType functionType, RemotePersistedValue remotePersistedValue);

  void setCurrentKey(Address address);
}
