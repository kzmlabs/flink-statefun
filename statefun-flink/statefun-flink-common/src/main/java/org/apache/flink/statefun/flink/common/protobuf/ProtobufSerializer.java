// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.common.protobuf;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.io.IOException;
import java.util.Objects;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.statefun.flink.common.generated.ProtobufSerializerSnapshot;

@NotThreadSafe
public final class ProtobufSerializer<M extends Message> {

  private final OutputStreamView output;
  private final CodedOutputStream codedOutputStream;
  private final InputStreamView input;
  private final CodedInputStream codedInputStream;
  private final Parser<M> parser;
  private final ProtobufSerializerSnapshot snapshot;

  public static <M extends Message> ProtobufSerializer<M> forMessageGeneratedClass(Class<M> type) {
    Objects.requireNonNull(type);
    Parser<M> parser = ProtobufReflectionUtil.protobufParser(type);
    ProtobufSerializerSnapshot snapshot = createSnapshot(type);
    return new ProtobufSerializer<>(parser, snapshot);
  }

  private ProtobufSerializer(Parser<M> parser, ProtobufSerializerSnapshot snapshot) {
    this.parser = Objects.requireNonNull(parser);
    this.snapshot = Objects.requireNonNull(snapshot);
    this.input = new InputStreamView();
    this.output = new OutputStreamView();
    this.codedInputStream = CodedInputStream.newInstance(input);
    this.codedOutputStream = CodedOutputStream.newInstance(output);
  }

  public void serialize(M record, DataOutputView target) throws IOException {
    final int size = record.getSerializedSize();
    target.writeInt(size);

    output.set(target);
    try {
      record.writeTo(codedOutputStream);
      codedOutputStream.flush();
    } finally {
      output.done();
    }
  }

  public M deserialize(DataInputView source) throws IOException {
    final int serializedSize = source.readInt();
    input.set(source, serializedSize);
    codedInputStream.resetSizeCounter();
    try {
      return parser.parseFrom(codedInputStream);
    } finally {
      input.done();
    }
  }

  public void copy(DataInputView source, DataOutputView target) throws IOException {
    int serializedSize = source.readInt();
    target.writeInt(serializedSize);
    target.write(source, serializedSize);
  }

  ProtobufSerializerSnapshot snapshot() {
    return snapshot;
  }

  public ProtobufSerializer<M> duplicate() {
    return new ProtobufSerializer<>(parser, snapshot);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------------------------------------------

  private static <M extends Message> ProtobufSerializerSnapshot createSnapshot(Class<M> type) {
    Descriptor messageDescriptor = ProtobufReflectionUtil.protobufDescriptor(type);
    FileDescriptorSet dependencies =
        ProtobufReflectionUtil.protoFileDescriptorSet(messageDescriptor);

    return ProtobufSerializerSnapshot.newBuilder()
        .setMessageName(messageDescriptor.getFullName())
        .setGeneratedJavaName(type.getName())
        .setDescriptorSet(dependencies)
        .build();
  }
}
