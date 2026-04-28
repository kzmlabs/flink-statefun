// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.state.processor.union;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Preconditions;

/** Type information for {@link TaggedBootstrapData}. */
public final class TaggedBootstrapDataTypeInfo extends TypeInformation<TaggedBootstrapData> {

  private static final long serialVersionUID = 1L;

  private final List<TypeInformation<?>> payloadTypeInfos;

  TaggedBootstrapDataTypeInfo(List<TypeInformation<?>> payloadTypeInfos) {
    Preconditions.checkNotNull(payloadTypeInfos);
    Preconditions.checkArgument(!payloadTypeInfos.isEmpty());
    this.payloadTypeInfos = payloadTypeInfos;
  }

  @Override
  public TypeSerializer<TaggedBootstrapData> createSerializer(SerializerConfig serializerConfig) {
    final List<TypeSerializer<?>> payloadSerializers =
        payloadTypeInfos.stream()
            .map(typeInfo -> typeInfo.createSerializer(serializerConfig))
            .collect(Collectors.toList());

    return new TaggedBootstrapDataSerializer(payloadSerializers);
  }

  @Override
  public int getTotalFields() {
    return 1;
  }

  @Override
  public int getArity() {
    return 1;
  }

  @Override
  public boolean isBasicType() {
    return false;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public boolean isTupleType() {
    return false;
  }

  @Override
  public Class<TaggedBootstrapData> getTypeClass() {
    return TaggedBootstrapData.class;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("TaggedBootstrapDataTypeInfo {");
    final int size = payloadTypeInfos.size();
    for (int i = 0; i < size; i++) {
      sb.append(payloadTypeInfos.get(i).toString());
      if (i < size - 1) {
        sb.append(", ");
      }
    }
    sb.append(" }");
    return sb.toString();
  }

  @Override
  public boolean canEqual(Object o) {
    return o instanceof TaggedBootstrapDataTypeInfo;
  }

  @Override
  public int hashCode() {
    return Objects.hash(payloadTypeInfos);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TaggedBootstrapDataTypeInfo that = (TaggedBootstrapDataTypeInfo) o;
    return Objects.equals(payloadTypeInfos, that.payloadTypeInfos);
  }
}
