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
import org.apache.flink.statefun.sdk.java.types.Types;
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
     * Header inserts never fail: headers are metadata and must not corrupt a production send. A
     * null value is preserved as a null-valued Kafka header ({@code has_value=false}), which Kafka
     * legally supports; a null key degrades to an empty key (Kafka itself forbids null keys, so
     * passing one through would fail at produce time inside the running job).
     */
    public Builder withUtf8Header(String key, String value) {
      return addHeader(key, value == null ? null : ByteString.copyFromUtf8(value));
    }

    public Builder withHeader(String key, byte[] value) {
      return addHeader(key, value == null ? null : ByteString.copyFrom(value));
    }

    public Builder withHeader(String key, Slice value) {
      return addHeader(key, value == null ? null : SliceProtobufUtil.asByteString(value));
    }

    public <T> Builder withHeader(String key, Type<T> type, T value) {
      if (type == null || value == null) {
        return addHeader(key, null);
      }
      TypeSerializer<T> serializer = type.typeSerializer();
      return withHeader(key, serializer.serialize(value));
    }

    /**
     * Primitive convenience overloads transfer the actual binary number (the SDK {@code Types}
     * encoding), equivalent to {@code withHeader(key, Types.integerType(), value)} — no text
     * round-trip. Read back with the matching {@code MessageHeader#valueAsInt()}-style accessor.
     * For text headers readable by generic Kafka tooling, use {@link #withUtf8Header}.
     */
    public Builder withHeader(String key, int value) {
      return withHeader(key, Types.integerType(), value);
    }

    public Builder withHeader(String key, long value) {
      return withHeader(key, Types.longType(), value);
    }

    public Builder withHeader(String key, float value) {
      return withHeader(key, Types.floatType(), value);
    }

    public Builder withHeader(String key, double value) {
      return withHeader(key, Types.doubleType(), value);
    }

    public Builder withHeader(String key, boolean value) {
      return withHeader(key, Types.booleanType(), value);
    }

    private Builder addHeader(String key, ByteString valueOrNull) {
      if (headers == null) {
        headers = new ArrayList<>();
      }
      KafkaProducerRecord.Header.Builder header =
          KafkaProducerRecord.Header.newBuilder().setKey(key == null ? "" : key);
      if (valueOrNull != null) {
        header.setValue(valueOrNull).setHasValue(true);
      }
      headers.add(header.build());
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
