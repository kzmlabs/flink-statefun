// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.httpfn.binders.v2;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;

import java.net.URL;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.httpfn.DefaultHttpRequestReplyClientFactory;
import org.apache.flink.statefun.flink.core.httpfn.TransportClientConstants;
import org.apache.flink.statefun.flink.core.message.MessageFactoryKey;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.junit.jupiter.api.Test;

public final class HttpEndpointBinderV2Test {
  private static final ObjectMapper OBJ_MAPPER = new ObjectMapper(new YAMLFactory());

  private static final String SPEC_YAML_PATH = "http-endpoint-binders/v2.yaml";

  @Test
  public void exampleUsage() throws Exception {
    final ComponentJsonObject component = loadComponentJsonObject(SPEC_YAML_PATH);
    final StatefulFunctionsUniverse universe = testUniverse();

    HttpEndpointBinderV2.INSTANCE.bind(component, universe);

    assertThat(universe.namespaceFunctions(), hasKey("com.foo.bar"));
  }

  private static ComponentJsonObject loadComponentJsonObject(String yamlPath) throws Exception {
    final URL url = HttpEndpointBinderV2Test.class.getClassLoader().getResource(yamlPath);
    final ObjectNode componentObject = OBJ_MAPPER.readValue(url, ObjectNode.class);
    return new ComponentJsonObject(componentObject);
  }

  private static StatefulFunctionsUniverse testUniverse() {
    final StatefulFunctionsUniverse universe =
        new StatefulFunctionsUniverse(
            MessageFactoryKey.forType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, null));
    universe.bindExtension(
        TransportClientConstants.OKHTTP_CLIENT_FACTORY_TYPE,
        DefaultHttpRequestReplyClientFactory.INSTANCE);
    return universe;
  }
}
