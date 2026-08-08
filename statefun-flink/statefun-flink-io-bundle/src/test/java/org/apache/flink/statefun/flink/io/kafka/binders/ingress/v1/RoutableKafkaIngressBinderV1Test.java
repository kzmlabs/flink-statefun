// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.io.kafka.binders.ingress.v1;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.Message;
import java.net.URL;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.flink.common.json.StateFunObjectMapper;
import org.apache.flink.statefun.flink.io.common.AutoRoutableProtobufRouter;
import org.apache.flink.statefun.flink.io.testutils.TestModuleBinder;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressSpec;
import org.junit.jupiter.api.Test;

public class RoutableKafkaIngressBinderV1Test {

  private static final ObjectMapper OBJ_MAPPER = new ObjectMapper(new YAMLFactory());

  private static final String SPEC_YAML_PATH = "kafka-io-binders/routable-kafka-ingress-v1.yaml";

  @Test
  public void exampleUsage() throws Exception {
    final ComponentJsonObject component = loadComponentJsonObject(SPEC_YAML_PATH);
    final TestModuleBinder testModuleBinder = new TestModuleBinder();

    RoutableKafkaIngressBinderV1.INSTANCE.bind(component, testModuleBinder);

    final IngressIdentifier<Message> expectedIngressId =
        new IngressIdentifier<>(Message.class, "com.foo.bar", "test-ingress");
    assertThat(testModuleBinder.getIngress(expectedIngressId), instanceOf(KafkaIngressSpec.class));
    assertThat(
        testModuleBinder.getRouters(expectedIngressId),
        hasItem(instanceOf(AutoRoutableProtobufRouter.class)));
  }

  @Test
  public void invalidRecordHandlingDefaultsToSkipWithWarn() throws Exception {
    RoutableKafkaIngressSpec spec = parseSpec(loadComponentJsonObject(SPEC_YAML_PATH));

    InvalidRecordPolicy policy = spec.invalidRecordPolicyByTopic().get("topic-1");

    assertThat(policy.action(), equalTo(InvalidRecordPolicy.Action.SKIP));
    assertThat(policy.logLevel(), equalTo(InvalidRecordPolicy.LogLevel.WARN));
  }

  @Test
  public void invalidRecordHandlingPerTopicOverrideWinsOverIngressDefault() throws Exception {
    RoutableKafkaIngressSpec spec = parseSpec(loadComponentJsonObject("kafka-io-binders/routable-kafka-ingress-v1-invalid-handling.yaml"));

    InvalidRecordPolicy strict = spec.invalidRecordPolicyByTopic().get("strict-topic");
    InvalidRecordPolicy lenient = spec.invalidRecordPolicyByTopic().get("lenient-topic");

    assertThat(strict.action(), equalTo(InvalidRecordPolicy.Action.FAIL));
    assertThat(lenient.action(), equalTo(InvalidRecordPolicy.Action.SKIP));
    assertThat(lenient.logLevel(), equalTo(InvalidRecordPolicy.LogLevel.ERROR));
  }

  @Test
  public void rejectsUnknownInvalidRecordHandlingType() {
    Exception e = assertThrows(Exception.class, () -> parseSpec(componentFromYaml("kind: io.statefun.kafka.v1/ingress\nspec:\n  id: com.foo.bar/x\n  invalidRecordHandling:\n    type: explode\n  topics:\n    - topic: t\n      valueType: com.googleapis/com.mycomp.foo.MessageA\n      targets:\n        - com.mycomp.foo/function-1\n")));

    assertThat(e.getMessage() + rootCauseMessage(e), org.hamcrest.Matchers.containsString("skip"));
    assertThat(e.getMessage() + rootCauseMessage(e), org.hamcrest.Matchers.containsString("fail"));
  }

  @Test
  public void emptyInvalidRecordHandlingNodeFallsBackToDefaults() throws Exception {
    RoutableKafkaIngressSpec spec = parseSpec(componentFromYaml("kind: io.statefun.kafka.v1/ingress\nspec:\n  id: com.foo.bar/x\n  invalidRecordHandling:\n  topics:\n    - topic: t\n      valueType: com.googleapis/com.mycomp.foo.MessageA\n      targets:\n        - com.mycomp.foo/function-1\n"));

    InvalidRecordPolicy policy = spec.invalidRecordPolicyByTopic().get("t");

    assertThat(policy.action(), equalTo(InvalidRecordPolicy.Action.SKIP));
    assertThat(policy.logLevel(), equalTo(InvalidRecordPolicy.LogLevel.WARN));
  }

  @Test
  public void rejectsLogLevelUnderFailPolicy() {
    assertThrows(Exception.class, () -> parseSpec(componentFromYaml("kind: io.statefun.kafka.v1/ingress\nspec:\n  id: com.foo.bar/x\n  invalidRecordHandling:\n    type: fail\n    logLevel: warn\n  topics:\n    - topic: t\n      valueType: com.googleapis/com.mycomp.foo.MessageA\n      targets:\n        - com.mycomp.foo/function-1\n")));
  }

  private static RoutableKafkaIngressSpec parseSpec(ComponentJsonObject component) throws Exception {
    return StateFunObjectMapper.create().treeToValue(component.specJsonNode(), RoutableKafkaIngressSpec.class);
  }

  private static ComponentJsonObject componentFromYaml(String yaml) throws Exception {
    return new ComponentJsonObject(OBJ_MAPPER.readValue(yaml, ObjectNode.class));
  }

  private static String rootCauseMessage(Throwable t) {
    while (t.getCause() != null) t = t.getCause();
    return String.valueOf(t.getMessage());
  }

  private static ComponentJsonObject loadComponentJsonObject(String yamlPath) throws Exception {
    final URL url = RoutableKafkaIngressBinderV1Test.class.getClassLoader().getResource(yamlPath);
    final ObjectNode componentObject = OBJ_MAPPER.readValue(url, ObjectNode.class);
    return new ComponentJsonObject(componentObject);
  }
}
