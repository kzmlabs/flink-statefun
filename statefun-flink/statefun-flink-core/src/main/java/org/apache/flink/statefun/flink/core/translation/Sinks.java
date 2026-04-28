// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.translation;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.common.Maps;
import org.apache.flink.statefun.flink.core.types.StaticallyRegisteredTypes;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

final class Sinks {
  private final Map<EgressIdentifier<?>, OutputTag<Object>> sideOutputs;
  private final Map<EgressIdentifier<?>, DecoratedSink> sinks;

  private Sinks(
      Map<EgressIdentifier<?>, OutputTag<Object>> sideOutputs,
      Map<EgressIdentifier<?>, DecoratedSink> sinks) {

    this.sideOutputs = Objects.requireNonNull(sideOutputs);
    this.sinks = Objects.requireNonNull(sinks);
  }

  static Sinks create(StatefulFunctionsUniverse universe) {
    return new Sinks(sideOutputs(universe), sinkFunctions(universe));
  }

  static Sinks create(
      StaticallyRegisteredTypes types, Iterable<EgressIdentifier<?>> egressIdentifiers) {
    SideOutputTranslator translator = new SideOutputTranslator(types, egressIdentifiers);
    return new Sinks(translator.translate(), Collections.emptyMap());
  }

  private static Map<EgressIdentifier<?>, DecoratedSink> sinkFunctions(
      StatefulFunctionsUniverse universe) {
    EgressToSinkTranslator egressTranslator = new EgressToSinkTranslator(universe);
    return egressTranslator.translate();
  }

  private static Map<EgressIdentifier<?>, OutputTag<Object>> sideOutputs(
      StatefulFunctionsUniverse universe) {
    SideOutputTranslator sideOutputTranslator = new SideOutputTranslator(universe);
    return sideOutputTranslator.translate();
  }

  Map<EgressIdentifier<?>, OutputTag<Object>> sideOutputTags() {
    return sideOutputs;
  }

  Map<EgressIdentifier<?>, DataStream<?>> sideOutputStreams(
      SingleOutputStreamOperator<?> mainOutput) {
    return Maps.transformValues(sideOutputs, (id, tag) -> mainOutput.getSideOutput(tag));
  }

  void consumeFrom(Map<EgressIdentifier<?>, DataStream<?>> sideOutputs) {
    sideOutputs.forEach(
        (egressIdentifier, rawSideOutputStream) -> {
          DecoratedSink decoratedSink = sinks.get(egressIdentifier);

          @SuppressWarnings("unchecked")
          Sink<Object> sink = (Sink<Object>) decoratedSink.sink;

          @SuppressWarnings("unchecked")
          DataStream<Object> sideOutputStream = (DataStream<Object>) rawSideOutputStream;

          DataStreamSink<Object> streamSink = sideOutputStream.sinkTo(sink);
          streamSink.name(decoratedSink.name);
          streamSink.uid(decoratedSink.uid);
        });
  }
}
