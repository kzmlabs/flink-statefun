// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.io.kafka.binders.ingress.v1;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.extensions.ComponentBinder;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.flink.common.json.StateFunObjectMapper;
import org.apache.flink.statefun.flink.io.common.AutoRoutableProtobufRouter;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

/**
 * Version 1 {@link ComponentBinder} for binding a Kafka ingress which automatically routes records
 * to target functions using the record key as the function id. Corresponding {@link TypeName} is
 * {@code io.statefun.kafka.v1/ingress}.
 *
 * <p>Below is an example YAML document of the {@link ComponentJsonObject} recognized by this
 * binder, with the expected types of each field:
 *
 * <pre>
 * kind: io.statefun.kafka.v1/ingress                                 (typename)
 * spec:                                                              (object)
 *   id: com.foo.bar/my-ingress                                       (typename)
 *   address: kafka-broker:9092                                       (string, optional)
 *   consumerGroupId: my-group-id                                     (string, optional)
 *   topics:                                                          (array)
 *   - topic: topic-1                                                 (string)
 *     valueType: com.foo.bar/my-type-1                               (typename)
 *     targets:                                                       (array)
 *       - com.mycomp.foo/function-1                                  (typename)
 *       - ...
 *   - ...
 *   autoOffsetResetPosition: earliest                                (string, optional)
 *   startupPosition:                                                 (object)
 *     type: earliest                                                 (string)
 *   properties:                                                      (array, optional)
 *     - foo.config: bar                                              (string)
 * </pre>
 *
 * <p>The {@code autoOffsetResetPosition} can be one of the following options: {@code earliest} or
 * {@code latest}.
 *
 * <p>Furthermore, the {@code startupPosition} can be of one of the following options: {@code
 * earliest}, {@code latest}, {@code group-offsets}, {@code specific-offsets}, or {@code date}.
 * Please see {@link RoutableKafkaIngressSpec} for further details.
 */
final class RoutableKafkaIngressBinderV1 implements ComponentBinder {

  private static final ObjectMapper SPEC_OBJ_MAPPER = StateFunObjectMapper.create();

  static final RoutableKafkaIngressBinderV1 INSTANCE = new RoutableKafkaIngressBinderV1();

  static final TypeName KIND_TYPE = TypeName.parseFrom("io.statefun.kafka.v1/ingress");

  private RoutableKafkaIngressBinderV1() {}

  @Override
  public void bind(
      ComponentJsonObject component, StatefulFunctionModule.Binder remoteModuleBinder) {
    validateComponent(component);

    final JsonNode specJsonNode = component.specJsonNode();
    final RoutableKafkaIngressSpec spec = parseSpec(specJsonNode);

    remoteModuleBinder.bindIngress(spec.toUniversalKafkaIngressSpec());
    remoteModuleBinder.bindIngressRouter(spec.id(), new AutoRoutableProtobufRouter());
  }

  private static void validateComponent(ComponentJsonObject componentJsonObject) {
    final TypeName targetBinderType = componentJsonObject.binderTypename();
    if (!targetBinderType.equals(KIND_TYPE)) {
      throw new IllegalStateException(
          "Received unexpected ModuleComponent to bind: " + componentJsonObject);
    }
  }

  private static RoutableKafkaIngressSpec parseSpec(JsonNode specJsonNode) {
    try {
      return SPEC_OBJ_MAPPER.treeToValue(specJsonNode, RoutableKafkaIngressSpec.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error parsing an AutoRoutableKafkaIngressSpec.", e);
    }
  }
}
