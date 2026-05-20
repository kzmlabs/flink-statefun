// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.e2e.k8s;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.e2e.k8s.generated.E2EProtos.CounterCommand;
import org.apache.flink.statefun.e2e.k8s.generated.E2EProtos.CounterResult;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;

/** Counter with short state TTL; used by the E2E suite to verify SDK-declared TTL is honored. */
public final class KafkaCounterTtlFn implements StatefulFunction {

  static final TypeName FN_TYPE = TypeName.typeNameOf("counter.kafka.ttl", "fn");
  static final TypeName EGRESS_ID = TypeName.typeNameOf("counter", "kafka-ttl-results");
  static final String RESULTS_TOPIC = "counter.results.ttl";

  static final Duration STATE_TTL = Duration.ofSeconds(5);

  static final ValueSpec<Long> TOTAL =
      ValueSpec.named("total").thatExpireAfterWrite(STATE_TTL).withLongType();

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
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic(RESULTS_TOPIC)
            .withUtf8Key(cmd.getId())
            .withValue(result.toByteArray())
            .build());

    return context.done();
  }
}
