// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.io.kinesis.binders.egress.v1;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.extensions.ComponentBinder;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.flink.common.json.StateFunObjectMapper;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.egress.generated.KinesisEgressRecord;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

/**
 * Version 1 {@link ComponentBinder} for {@code io.statefun.kinesis.v1/egress}. Accepts {@link
 * KinesisEgressRecord} and writes the wrapped value bytes to Kinesis.
 *
 * <p>YAML schema reference: see {@code docs/kinesis-io.md}.
 */
final class GenericKinesisEgressBinderV1 implements ComponentBinder {

  private static final ObjectMapper SPEC_OBJ_MAPPER = StateFunObjectMapper.create();

  static final GenericKinesisEgressBinderV1 INSTANCE = new GenericKinesisEgressBinderV1();

  static final TypeName KIND_TYPE = TypeName.parseFrom("io.statefun.kinesis.v1/egress");

  private GenericKinesisEgressBinderV1() {}

  @Override
  public void bind(
      ComponentJsonObject component, StatefulFunctionModule.Binder remoteModuleBinder) {
    validateComponent(component);

    final JsonNode specJsonNode = component.specJsonNode();
    final GenericKinesisEgressSpec spec = parseSpec(specJsonNode);
    remoteModuleBinder.bindEgress(spec.toUniversalKinesisEgressSpec());
  }

  private static void validateComponent(ComponentJsonObject componentJsonObject) {
    final TypeName targetBinderType = componentJsonObject.binderTypename();
    if (!targetBinderType.equals(KIND_TYPE)) {
      throw new IllegalStateException(
          "Received unexpected ModuleComponent to bind: " + componentJsonObject);
    }
  }

  private static GenericKinesisEgressSpec parseSpec(JsonNode specJsonNode) {
    try {
      return SPEC_OBJ_MAPPER.treeToValue(specJsonNode, GenericKinesisEgressSpec.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error parsing a GenericKinesisEgressSpec.", e);
    }
  }
}
