// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.io.kinesis.binders.egress.v1;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.statefun.flink.common.types.TypedValueUtil;
import org.apache.flink.statefun.sdk.egress.generated.KinesisEgressRecord;
import org.apache.flink.statefun.sdk.kinesis.egress.EgressRecord;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSerializer;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public final class GenericKinesisEgressSerializer implements KinesisEgressSerializer<TypedValue> {

  private static final long serialVersionUID = 1L;

  @Override
  public EgressRecord serialize(TypedValue value) {
    final KinesisEgressRecord kinesisEgressRecord = asKinesisEgressRecord(value);

    final EgressRecord.Builder builder =
        EgressRecord.newBuilder()
            .withData(kinesisEgressRecord.getValueBytes().toByteArray())
            .withStream(kinesisEgressRecord.getStream())
            .withPartitionKey(kinesisEgressRecord.getPartitionKey());

    final String explicitHashKey = kinesisEgressRecord.getExplicitHashKey();
    if (explicitHashKey != null && !explicitHashKey.isEmpty()) {
      builder.withExplicitHashKey(explicitHashKey);
    }

    return builder.build();
  }

  private static KinesisEgressRecord asKinesisEgressRecord(TypedValue message) {
    if (!TypedValueUtil.isProtobufTypeOf(message, KinesisEgressRecord.getDescriptor())) {
      throw new IllegalStateException(
          "The generic Kinesis egress expects only messages of type "
              + KinesisEgressRecord.class.getName());
    }
    try {
      return KinesisEgressRecord.parseFrom(message.getValue());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          "Unable to unpack message as a " + KinesisEgressRecord.class.getName(), e);
    }
  }
}
