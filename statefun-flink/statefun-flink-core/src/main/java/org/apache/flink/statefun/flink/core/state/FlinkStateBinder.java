// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.state;

import java.util.Objects;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.state.Accessor;
import org.apache.flink.statefun.sdk.state.ApiExtension;
import org.apache.flink.statefun.sdk.state.AppendingBufferAccessor;
import org.apache.flink.statefun.sdk.state.PersistedAppendingBuffer;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import org.apache.flink.statefun.sdk.state.RemotePersistedValue;
import org.apache.flink.statefun.sdk.state.StateBinder;
import org.apache.flink.statefun.sdk.state.TableAccessor;

/**
 * A {@link StateBinder} that binds persisted state objects to Flink state for a specific {@link
 * FunctionType}.
 */
public final class FlinkStateBinder extends StateBinder {
  private final State state;
  private final FunctionType functionType;

  @Inject
  public FlinkStateBinder(State state, FunctionType functionType) {
    this.state = Objects.requireNonNull(state);
    this.functionType = Objects.requireNonNull(functionType);
  }

  @Override
  public void bind(Object stateObject) {
    if (stateObject instanceof PersistedValue) {
      bindValue((PersistedValue<?>) stateObject);
    } else if (stateObject instanceof PersistedTable) {
      bindTable((PersistedTable<?, ?>) stateObject);
    } else if (stateObject instanceof PersistedAppendingBuffer) {
      bindAppendingBuffer((PersistedAppendingBuffer<?>) stateObject);
    } else if (stateObject instanceof RemotePersistedValue) {
      bindRemoteValue((RemotePersistedValue) stateObject);
    } else {
      throw new IllegalArgumentException("Unknown persisted state object " + stateObject);
    }
  }

  private void bindValue(PersistedValue<?> persistedValue) {
    Accessor<?> accessor = state.createFlinkStateAccessor(functionType, persistedValue);
    setAccessorRaw(persistedValue, accessor);
  }

  private void bindTable(PersistedTable<?, ?> persistedTable) {
    TableAccessor<?, ?> accessor =
        state.createFlinkStateTableAccessor(functionType, persistedTable);
    setAccessorRaw(persistedTable, accessor);
  }

  private void bindAppendingBuffer(PersistedAppendingBuffer<?> persistedAppendingBuffer) {
    AppendingBufferAccessor<?> accessor =
        state.createFlinkStateAppendingBufferAccessor(functionType, persistedAppendingBuffer);
    setAccessorRaw(persistedAppendingBuffer, accessor);
  }

  private void bindRemoteValue(RemotePersistedValue remotePersistedValue) {
    Accessor<byte[]> accessor =
        state.createFlinkRemoteStateAccessor(functionType, remotePersistedValue);
    setAccessorRaw(remotePersistedValue, accessor);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void setAccessorRaw(PersistedTable<?, ?> persistedTable, TableAccessor<?, ?> accessor) {
    ApiExtension.setPersistedTableAccessor((PersistedTable) persistedTable, accessor);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void setAccessorRaw(PersistedValue<?> persistedValue, Accessor<?> accessor) {
    ApiExtension.setPersistedValueAccessor((PersistedValue) persistedValue, accessor);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void setAccessorRaw(
      PersistedAppendingBuffer<?> persistedAppendingBuffer, AppendingBufferAccessor<?> accessor) {
    ApiExtension.setPersistedAppendingBufferAccessor(
        (PersistedAppendingBuffer) persistedAppendingBuffer, accessor);
  }

  private static void setAccessorRaw(
      RemotePersistedValue remotePersistedValue, Accessor<byte[]> accessor) {
    ApiExtension.setRemotePersistedValueAccessor(remotePersistedValue, accessor);
  }
}
