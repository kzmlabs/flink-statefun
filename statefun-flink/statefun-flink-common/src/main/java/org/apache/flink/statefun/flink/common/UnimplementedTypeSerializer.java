// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.common;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

final class UnimplementedTypeSerializer<T> extends TypeSerializer<T> {

  private static final long serialVersionUID = 1L;

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<T> duplicate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public T createInstance() {
    throw new UnsupportedOperationException();
  }

  @Override
  public T copy(T t) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T copy(T t, T t1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getLength() {
    return 0;
  }

  @Override
  public void serialize(T t, DataOutputView dataOutputView) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T deserialize(DataInputView dataInputView) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T deserialize(T t, DataInputView dataInputView) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void copy(DataInputView dataInputView, DataOutputView dataOutputView) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object o) {
    return o == this;
  }

  @Override
  public int hashCode() {
    return 7;
  }

  @Override
  public TypeSerializerSnapshot<T> snapshotConfiguration() {
    throw new UnsupportedOperationException();
  }
}
