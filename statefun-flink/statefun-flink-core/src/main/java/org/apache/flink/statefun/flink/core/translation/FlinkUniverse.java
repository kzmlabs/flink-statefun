// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.translation;

import java.util.Map;
import java.util.Objects;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.feedback.FeedbackKey;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class FlinkUniverse {

  private final StatefulFunctionsUniverse universe;

  private final StatefulFunctionsConfig configuration;
  private final FeedbackKey<Message> feedbackKey;

  public FlinkUniverse(
      FeedbackKey<Message> feedbackKey,
      StatefulFunctionsConfig configuration,
      StatefulFunctionsUniverse universe) {
    this.feedbackKey = Objects.requireNonNull(feedbackKey);
    this.universe = Objects.requireNonNull(universe);
    this.configuration = Objects.requireNonNull(configuration);
  }

  public void configure(StreamExecutionEnvironment env) {
    Sources sources = Sources.create(env, universe, configuration);
    Sinks sinks = Sinks.create(universe);

    StatefulFunctionTranslator translator =
        new StatefulFunctionTranslator(feedbackKey, configuration);

    Map<EgressIdentifier<?>, DataStream<?>> sideOutputs = translator.translate(sources, sinks);

    sinks.consumeFrom(sideOutputs);
  }
}
