// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.e2e.k8s;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.e2e.k8s.generated.E2EProtos.CounterCommand;
import org.apache.flink.statefun.e2e.k8s.generated.E2EProtos.CounterResult;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

/** Counter function: consumes CounterCommand from Kafka, emits CounterResult to Kafka. */
public final class KafkaCounterFn implements StatefulFunction {

  static final TypeName FN_TYPE = TypeName.typeNameOf("counter.kafka", "fn");
  static final TypeName EGRESS_ID = TypeName.typeNameOf("counter", "kafka-results");
  static final String RESULTS_TOPIC = "counter.results";

  static final ValueSpec<Long> TOTAL = ValueSpec.named("total").withLongType();

  static final TypeName COUNTER_COMMAND_TYPE_NAME =
      TypeName.typeNameOf("io.github.kzmlabs.statefun.e2e", "CounterCommand");

  static final Type<CounterCommand> COUNTER_COMMAND_TYPE =
      SimpleType.simpleTypeFrom(
          COUNTER_COMMAND_TYPE_NAME, CounterCommand::toByteArray, CounterCommand::parseFrom);

  @Override
  public CompletableFuture<Void> apply(Context context, Message message) {
    if (!message.is(COUNTER_COMMAND_TYPE)) {
      return context.done();
    }

    CounterCommand cmd = message.as(COUNTER_COMMAND_TYPE);
    long newTotal = context.storage().get(TOTAL).orElse(0L) + cmd.getDelta();
    context.storage().set(TOTAL, newTotal);

    CounterResult result =
        CounterResult.newBuilder().setId(cmd.getId()).setTotal(newTotal).build();

    context.send(
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic(RESULTS_TOPIC)
            .withUtf8Key(cmd.getId())
            .withValue(result.toByteArray())
            .build());

    return context.done();
  }
}
