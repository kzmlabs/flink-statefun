// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.translation;

import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.StatefulFunctionsJobConstants;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.common.Maps;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.streaming.api.datastream.DataStream;

final class RouterTranslator {
  private final StatefulFunctionsUniverse universe;

  private final StatefulFunctionsConfig configuration;

  RouterTranslator(StatefulFunctionsUniverse universe, StatefulFunctionsConfig configuration) {
    this.universe = universe;
    this.configuration = configuration;
  }

  Map<IngressIdentifier<?>, DataStream<Message>> translate(
      Map<IngressIdentifier<?>, DataStream<?>> sources) {
    return Maps.transformValues(
        universe.routers(), (id, unused) -> createRoutersForSource(id, sources.get(id)));
  }

  /**
   * For each input {@linkplain DataStream} (created as a result of {@linkplain IngressSpec}
   * translation) we attach a single FlatMap function that would invoke all the defined routers for
   * that spec. Please note that the FlatMap function must have the same parallelism as the
   * {@linkplain DataStream} it is attached to, so that we keep per key ordering.
   */
  @SuppressWarnings("unchecked")
  private DataStream<Message> createRoutersForSource(
      IngressIdentifier<?> id, DataStream<?> sourceStream) {
    IngressIdentifier<Object> castedId = (IngressIdentifier<Object>) id;
    DataStream<Object> castedSource = (DataStream<Object>) sourceStream;

    IngressRouterOperator<Object> router = new IngressRouterOperator<>(configuration, castedId);

    TypeInformation<Message> typeInfo = universe.types().registerType(Message.class);

    int sourceParallelism = castedSource.getParallelism();

    String operatorName = StatefulFunctionsJobConstants.ROUTER_NAME + " (" + castedId.name() + ")";
    return castedSource
        .transform(operatorName, typeInfo, router)
        .setParallelism(sourceParallelism)
        .uid(routerUID(id))
        .returns(typeInfo);
  }

  private String routerUID(IngressIdentifier<?> identifier) {
    return String.format("%s-%s-router", identifier.namespace(), identifier.name());
  }
}
