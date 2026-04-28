// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.translation;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverseProvider;
import org.apache.flink.statefun.flink.core.feedback.FeedbackKey;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.core.types.StaticallyRegisteredTypes;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.streaming.api.datastream.DataStream;

public class EmbeddedTranslator {
  private final StatefulFunctionsConfig configuration;
  private final FeedbackKey<Message> feedbackKey;

  public EmbeddedTranslator(StatefulFunctionsConfig config, FeedbackKey<Message> feedbackKey) {
    this.configuration = config;
    this.feedbackKey = feedbackKey;
  }

  public <T extends StatefulFunctionProvider & Serializable>
      Map<EgressIdentifier<?>, DataStream<?>> translate(
          List<DataStream<RoutableMessage>> ingresses,
          Iterable<EgressIdentifier<?>> egressesIds,
          Map<FunctionType, T> functions) {

    configuration.setProvider(new EmbeddedUniverseProvider<>(functions));

    StaticallyRegisteredTypes types = new StaticallyRegisteredTypes(configuration.getFactoryKey());
    Sources sources = Sources.create(types, ingresses);
    Sinks sinks = Sinks.create(types, egressesIds);

    StatefulFunctionTranslator translator =
        new StatefulFunctionTranslator(feedbackKey, configuration);

    return translator.translate(sources, sinks);
  }

  private static class EmbeddedUniverseProvider<T extends StatefulFunctionProvider & Serializable>
      implements StatefulFunctionsUniverseProvider {

    private static final long serialVersionUID = 1;

    private Map<FunctionType, T> functions;

    public EmbeddedUniverseProvider(Map<FunctionType, T> functions) {
      this.functions = Objects.requireNonNull(functions);
    }

    @Override
    public StatefulFunctionsUniverse get(
        ClassLoader classLoader, StatefulFunctionsConfig configuration) {
      StatefulFunctionsUniverse u = new StatefulFunctionsUniverse(configuration.getFactoryKey());
      functions.forEach(u::bindFunctionProvider);
      return u;
    }
  }
}
