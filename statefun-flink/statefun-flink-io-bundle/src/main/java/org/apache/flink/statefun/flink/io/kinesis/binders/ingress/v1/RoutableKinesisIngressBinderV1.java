/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.statefun.flink.io.kinesis.binders.ingress.v1;

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
 * Version 1 {@link ComponentBinder} for {@code io.statefun.kinesis.v1/ingress}. Records are
 * auto-routed to target functions by record key.
 *
 * <p>YAML schema reference: see {@code docs/kinesis-io.md}.
 */
final class RoutableKinesisIngressBinderV1 implements ComponentBinder {

  private static final ObjectMapper SPEC_OBJ_MAPPER = StateFunObjectMapper.create();

  static final RoutableKinesisIngressBinderV1 INSTANCE = new RoutableKinesisIngressBinderV1();

  static final TypeName KIND_TYPE = TypeName.parseFrom("io.statefun.kinesis.v1/ingress");

  private RoutableKinesisIngressBinderV1() {}

  @Override
  public void bind(
      ComponentJsonObject component, StatefulFunctionModule.Binder remoteModuleBinder) {
    validateComponent(component);

    final JsonNode specJsonNode = component.specJsonNode();
    final RoutableKinesisIngressSpec spec = parseSpec(specJsonNode);

    remoteModuleBinder.bindIngress(spec.toUniversalKinesisIngressSpec());
    remoteModuleBinder.bindIngressRouter(spec.id(), new AutoRoutableProtobufRouter());
  }

  private static void validateComponent(ComponentJsonObject componentJsonObject) {
    final TypeName targetBinderType = componentJsonObject.binderTypename();
    if (!targetBinderType.equals(KIND_TYPE)) {
      throw new IllegalStateException(
          "Received unexpected ModuleComponent to bind: " + componentJsonObject);
    }
  }

  private static RoutableKinesisIngressSpec parseSpec(JsonNode specJsonNode) {
    try {
      return SPEC_OBJ_MAPPER.treeToValue(specJsonNode, RoutableKinesisIngressSpec.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error parsing an AutoRoutableKinesisIngressSpec.", e);
    }
  }
}
