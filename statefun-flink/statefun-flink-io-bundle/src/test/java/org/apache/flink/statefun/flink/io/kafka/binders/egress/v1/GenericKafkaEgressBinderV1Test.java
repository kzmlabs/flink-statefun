// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.io.kafka.binders.egress.v1;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import java.net.URL;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.flink.io.testutils.TestModuleBinder;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSpec;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.junit.jupiter.api.Test;

public class GenericKafkaEgressBinderV1Test {

  private static final ObjectMapper OBJ_MAPPER = new ObjectMapper(new YAMLFactory());

  private static final String SPEC_YAML_PATH = "kafka-io-binders/generic-kafka-egress-v1.yaml";

  @Test
  public void exampleUsage() throws Exception {
    final ComponentJsonObject component = loadComponentJsonObject(SPEC_YAML_PATH);
    final TestModuleBinder testModuleBinder = new TestModuleBinder();

    GenericKafkaEgressBinderV1.INSTANCE.bind(component, testModuleBinder);

    final EgressIdentifier<TypedValue> expectedEgressId =
        new EgressIdentifier<>("com.foo.bar", "test-egress", TypedValue.class);
    assertThat(testModuleBinder.getEgress(expectedEgressId), instanceOf(KafkaEgressSpec.class));
  }

  private static ComponentJsonObject loadComponentJsonObject(String yamlPath) throws Exception {
    final URL url = GenericKafkaEgressBinderV1Test.class.getClassLoader().getResource(yamlPath);
    final ObjectNode componentObject = OBJ_MAPPER.readValue(url, ObjectNode.class);
    return new ComponentJsonObject(componentObject);
  }
}
