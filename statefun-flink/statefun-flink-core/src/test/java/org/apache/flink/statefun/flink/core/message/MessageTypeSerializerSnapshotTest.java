// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.stream.Stream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class MessageTypeSerializerSnapshotTest {

  private static final String serializerClassName = "com.domain.Serializer";

  private static class SnapshotData {
    public int version;
    public byte[] bytes;
  }

  private interface SnapshotDataProvider {
    SnapshotData provide(MessageFactoryKey messageFactoryKey) throws IOException;
  }

  static Stream<Arguments> data() {

    MessageFactoryKey kryoFactoryKey =
        MessageFactoryKey.forType(MessageFactoryType.WITH_KRYO_PAYLOADS, null);
    MessageFactoryKey customFactoryKey =
        MessageFactoryKey.forType(MessageFactoryType.WITH_CUSTOM_PAYLOADS, serializerClassName);

    // generates snapshot data for V1, without customPayloadSerializerClassName
    SnapshotDataProvider snapshotDataProviderV1 =
        messageFactoryKey -> {
          try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            DataOutputView dataOutputView = new DataOutputViewStreamWrapper(bos);
            dataOutputView.writeUTF(messageFactoryKey.getType().name());
            return new SnapshotData() {
              {
                version = 1;
                bytes = bos.toByteArray();
              }
            };
          }
        };

    // generates snapshot data for V2, the current version
    SnapshotDataProvider snapshotDataProviderV2 =
        messageFactoryKey -> {
          MessageTypeSerializer.Snapshot snapshot =
              new MessageTypeSerializer.Snapshot(messageFactoryKey);
          try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            DataOutputView dataOutputView = new DataOutputViewStreamWrapper(bos);
            snapshot.writeSnapshot(dataOutputView);
            return new SnapshotData() {
              {
                version = 2;
                bytes = bos.toByteArray();
              }
            };
          }
        };

    return Stream.of(
        Arguments.of(kryoFactoryKey, snapshotDataProviderV1),
        Arguments.of(kryoFactoryKey, snapshotDataProviderV2),
        Arguments.of(customFactoryKey, snapshotDataProviderV2));
  }

  @ParameterizedTest
  @MethodSource("data")
  void roundTrip(MessageFactoryKey messageFactoryKey, SnapshotDataProvider snapshotDataProvider)
      throws IOException {

    SnapshotData snapshotData = snapshotDataProvider.provide(messageFactoryKey);
    MessageTypeSerializer.Snapshot snapshot = new MessageTypeSerializer.Snapshot(messageFactoryKey);
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    try (ByteArrayInputStream bis = new ByteArrayInputStream(snapshotData.bytes)) {
      DataInputView dataInputView = new DataInputViewStreamWrapper(bis);
      snapshot.readSnapshot(snapshotData.version, dataInputView, classLoader);
    }

    // make sure the deserialized state matches what was used to serialize
    assert (snapshot.getMessageFactoryKey().equals(messageFactoryKey));
  }
}
