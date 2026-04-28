// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.e2e.k8s;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.e2e.k8s.generated.E2EProtos.CounterCommand;
import org.apache.flink.statefun.e2e.k8s.generated.E2EProtos.CounterResult;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.io.KinesisEgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;

/** Counter function: consumes CounterCommand from Kinesis, emits CounterResult to Kinesis. */
public final class KinesisCounterFn implements StatefulFunction {

  static final TypeName FN_TYPE = TypeName.typeNameOf("counter.kinesis", "fn");
  static final TypeName EGRESS_ID = TypeName.typeNameOf("counter", "kinesis-results");
  static final String RESULTS_STREAM = "counter.results";

  static final ValueSpec<Long> TOTAL = ValueSpec.named("total").withLongType();

  @Override
  public CompletableFuture<Void> apply(Context context, Message message) {
    if (!message.is(KafkaCounterFn.COUNTER_COMMAND_TYPE)) {
      return context.done();
    }

    CounterCommand cmd = message.as(KafkaCounterFn.COUNTER_COMMAND_TYPE);
    long newTotal = context.storage().get(TOTAL).orElse(0L) + cmd.getDelta();
    context.storage().set(TOTAL, newTotal);

    CounterResult result =
        CounterResult.newBuilder().setId(cmd.getId()).setTotal(newTotal).build();

    context.send(
        KinesisEgressMessage.forEgress(EGRESS_ID)
            .withStream(RESULTS_STREAM)
            .withUtf8PartitionKey(cmd.getId())
            .withValue(result.toByteArray())
            .build());

    return context.done();
  }
}
