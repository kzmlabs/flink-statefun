// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.common.protobuf;

import com.google.protobuf.Message;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Objects;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public final class ProtobufTypeSerializer<M extends Message> extends TypeSerializer<M> {

  private static final long serialVersionUID = 1;

  private final Class<M> typeClass;
  private transient ProtobufSerializer<M> underlyingSerializer;

  /** this is a lazy computed snapshot */
  @SuppressWarnings("InstanceVariableMayNotBeInitializedByReadObject")
  private transient ProtobufTypeSerializerSnapshot<M> snapshot;

  // --------------------------------------------------------------------------------------------------
  // Constructors
  // --------------------------------------------------------------------------------------------------

  ProtobufTypeSerializer(Class<M> typeClass) {
    this(typeClass, ProtobufSerializer.forMessageGeneratedClass(typeClass));
  }

  private ProtobufTypeSerializer(Class<M> typeClass, ProtobufSerializer<M> protobufSerializer) {
    this.typeClass = Objects.requireNonNull(typeClass);
    this.underlyingSerializer = Objects.requireNonNull(protobufSerializer);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    this.underlyingSerializer = ProtobufSerializer.forMessageGeneratedClass(typeClass);
  }

  @Override
  public TypeSerializer<M> duplicate() {
    return new ProtobufTypeSerializer<>(typeClass, underlyingSerializer.duplicate());
  }

  @Override
  public boolean isImmutableType() {
    return true;
  }

  @Override
  public M createInstance() {
    return null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public M copy(M from) {
    return (M) from.toBuilder().build();
  }

  @Override
  public M copy(M from, M reuse) {
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(M record, DataOutputView target) throws IOException {
    underlyingSerializer.serialize(record, target);
  }

  @Override
  public M deserialize(DataInputView source) throws IOException {
    return underlyingSerializer.deserialize(source);
  }

  @Override
  public M deserialize(M reuse, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    underlyingSerializer.copy(source, target);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    Class<?> aClass = obj.getClass();
    return getClass().equals(aClass);
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public TypeSerializerSnapshot<M> snapshotConfiguration() {
    ProtobufTypeSerializerSnapshot<M> snapshot = this.snapshot;
    if (snapshot == null) {
      snapshot = new ProtobufTypeSerializerSnapshot<>(typeClass, underlyingSerializer.snapshot());
      this.snapshot = snapshot;
    }
    return snapshot;
  }

  Class<M> getTypeClass() {
    return typeClass;
  }
}
