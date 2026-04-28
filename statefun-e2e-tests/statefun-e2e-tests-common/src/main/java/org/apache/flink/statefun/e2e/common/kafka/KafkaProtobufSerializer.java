// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.e2e.common.kafka;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A Kafka {@link Serializer} and {@link Deserializer} that uses Protobuf for serialization.
 *
 * @param <T> type of the Protobuf message.
 */
public final class KafkaProtobufSerializer<T extends Message>
    implements Serializer<T>, Deserializer<T> {

  private final Parser<T> parser;

  public KafkaProtobufSerializer(Parser<T> parser) {
    this.parser = Objects.requireNonNull(parser);
  }

  @Override
  public byte[] serialize(String s, T command) {
    return command.toByteArray();
  }

  @Override
  public T deserialize(String s, byte[] bytes) {
    try {
      return parser.parseFrom(bytes);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> map, boolean b) {}
}
