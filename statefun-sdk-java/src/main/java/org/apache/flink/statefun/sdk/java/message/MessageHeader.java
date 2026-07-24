// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java.message;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.flink.statefun.sdk.java.slice.Slice;

/**
 * A single transport-level header attached to an incoming {@link Message}, e.g. a Kafka record
 * header of the record that produced this message. Header keys are not unique: multiple headers
 * may share the same key, and their original order is preserved.
 */
public final class MessageHeader {
  private final String key;
  private final Slice value;

  public MessageHeader(String key, Slice value) {
    this.key = Objects.requireNonNull(key);
    this.value = Objects.requireNonNull(value);
  }

  public String key() {
    return key;
  }

  public Slice value() {
    return value;
  }

  public String valueAsUtf8String() {
    return new String(value.toByteArray(), StandardCharsets.UTF_8);
  }

  @Override
  public String toString() {
    return "MessageHeader{key='" + key + "', value=" + value.readableBytes() + " bytes}";
  }
}
