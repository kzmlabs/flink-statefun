// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java.message;

import java.nio.charset.StandardCharsets;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.statefun.sdk.java.types.Types;

/**
 * A single transport-level header attached to an incoming {@link Message}, e.g. a Kafka record
 * header of the record that produced this message. Header keys are not unique: multiple headers
 * may share the same key, and their original order is preserved.
 *
 * <p>Never throws on degenerate input: this type materializes on the message read path inside
 * live function invocations, so a null key degrades to an empty key, and a null value — legal in
 * Kafka, see {@code org.apache.kafka.common.header.Header#value()} — is preserved as {@code
 * null}.
 */
public final class MessageHeader {
  private final String key;
  private final Slice value;

  public MessageHeader(String key, Slice value) {
    this.key = key == null ? "" : key;
    this.value = value;
  }

  public String key() {
    return key;
  }

  /** True when the header carries a value; false when the Kafka header value was null. */
  public boolean hasValue() {
    return value != null;
  }

  /** The header value, or {@code null} when the Kafka header value was null. */
  public Slice value() {
    return value;
  }

  /** The header value decoded as UTF-8, or {@code null} when the header value was null. */
  public String valueAsUtf8String() {
    return value == null ? null : new String(value.toByteArray(), StandardCharsets.UTF_8);
  }

  /**
   * Decodes the header value with the given SDK {@link Type}, the read-side counterpart of {@code
   * KafkaEgressMessage.Builder#withHeader(String, Type, Object)}. Prod-safe by design: a null or
   * undecodable value yields {@code null}, never an exception.
   */
  public <T> T valueAs(Type<T> type) {
    if (value == null || type == null) {
      return null;
    }
    try {
      return type.typeSerializer().deserialize(value);
    } catch (RuntimeException e) {
      return null;
    }
  }

  /**
   * Primitive accessors decode the binary SDK {@code Types} encodings written by the matching
   * {@code withHeader(key, 10)}-style overloads. Same prod-safe contract as {@link
   * #valueAs(Type)}: null or undecodable values yield {@code null}.
   */
  public Integer valueAsInt() {
    return valueAs(Types.integerType());
  }

  public Long valueAsLong() {
    return valueAs(Types.longType());
  }

  public Double valueAsDouble() {
    return valueAs(Types.doubleType());
  }

  public Boolean valueAsBoolean() {
    return valueAs(Types.booleanType());
  }

  @Override
  public String toString() {
    return "MessageHeader{key='"
        + key
        + "', value="
        + (value == null ? "null" : value.readableBytes() + " bytes")
        + '}';
  }
}
