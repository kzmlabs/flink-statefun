// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.java.io;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.java.ApiExtension;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessageWrapper;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.statefun.sdk.java.types.TypeSerializer;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.ByteString;

public final class KafkaEgressMessage {

  public static Builder forEgress(TypeName targetEgressId) {
    Objects.requireNonNull(targetEgressId);
    return new Builder(targetEgressId);
  }

  public static final class Builder {
    private static final TypeName KAFKA_PRODUCER_RECORD_TYPENAME =
        TypeName.typeNameOf(
            "type.googleapis.com", KafkaProducerRecord.getDescriptor().getFullName());

    private final TypeName targetEgressId;
    private ByteString targetTopic;
    private ByteString keyBytes;
    private ByteString value;
    private List<KafkaProducerRecord.Header> headers;

    private Builder(TypeName targetEgressId) {
      this.targetEgressId = targetEgressId;
    }

    public Builder withTopic(String topic) {
      this.targetTopic = ByteString.copyFromUtf8(topic);
      return this;
    }

    public Builder withTopic(Slice topic) {
      this.targetTopic = SliceProtobufUtil.asByteString(topic);
      return this;
    }

    public Builder withUtf8Key(String key) {
      Objects.requireNonNull(key);
      this.keyBytes = ByteString.copyFromUtf8(key);
      return this;
    }

    public Builder withKey(byte[] key) {
      Objects.requireNonNull(key);
      this.keyBytes = ByteString.copyFrom(key);
      return this;
    }

    public Builder withKey(Slice slice) {
      Objects.requireNonNull(slice);
      this.keyBytes = SliceProtobufUtil.asByteString(slice);
      return this;
    }

    public <T> Builder withKey(Type<T> type, T value) {
      TypeSerializer<T> serializer = type.typeSerializer();
      return withKey(serializer.serialize(value));
    }

    public Builder withUtf8Value(String value) {
      Objects.requireNonNull(value);
      this.value = ByteString.copyFromUtf8(value);
      return this;
    }

    public Builder withValue(Slice slice) {
      Objects.requireNonNull(slice);
      this.value = SliceProtobufUtil.asByteString(slice);
      return this;
    }

    public <T> Builder withValue(Type<T> type, T value) {
      TypeSerializer<T> serializer = type.typeSerializer();
      return withValue(serializer.serialize(value));
    }

    public Builder withValue(byte[] value) {
      Objects.requireNonNull(value);
      this.value = ByteString.copyFrom(value);
      return this;
    }

    /**
     * Header values are null-tolerant: Kafka permits headers without a value, so a {@code null}
     * value is carried as empty bytes (protobuf cannot represent null) instead of failing the
     * build. Header keys must be non-null, matching Kafka's own contract.
     */
    public Builder withUtf8Header(String key, String value) {
      return addHeader(key, value == null ? ByteString.EMPTY : ByteString.copyFromUtf8(value));
    }

    public Builder withHeader(String key, byte[] value) {
      return addHeader(key, value == null ? ByteString.EMPTY : ByteString.copyFrom(value));
    }

    public Builder withHeader(String key, Slice value) {
      return addHeader(key, value == null ? ByteString.EMPTY : SliceProtobufUtil.asByteString(value));
    }

    public <T> Builder withHeader(String key, Type<T> type, T value) {
      if (value == null) {
        return addHeader(key, ByteString.EMPTY);
      }
      TypeSerializer<T> serializer = type.typeSerializer();
      return withHeader(key, serializer.serialize(value));
    }

    private Builder addHeader(String key, ByteString value) {
      Objects.requireNonNull(key);
      if (headers == null) {
        headers = new ArrayList<>();
      }
      headers.add(KafkaProducerRecord.Header.newBuilder().setKey(key).setValue(value).build());
      return this;
    }

    public EgressMessage build() {
      if (targetTopic == null) {
        throw new IllegalStateException("A Kafka record requires a target topic.");
      }
      if (value == null) {
        throw new IllegalStateException("A Kafka record requires value bytes");
      }
      KafkaProducerRecord.Builder builder =
          KafkaProducerRecord.newBuilder().setTopicBytes(targetTopic).setValueBytes(value);
      if (keyBytes != null) {
        builder.setKeyBytes(keyBytes);
      }
      if (headers != null) {
        builder.addAllHeaders(headers);
      }
      KafkaProducerRecord record = builder.build();
      TypedValue typedValue =
          TypedValue.newBuilder()
              .setTypenameBytes(ApiExtension.typeNameByteString(KAFKA_PRODUCER_RECORD_TYPENAME))
              .setValue(record.toByteString())
              .setHasValue(true)
              .build();

      return new EgressMessageWrapper(targetEgressId, typedValue);
    }
  }
}
