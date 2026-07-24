// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.io.kafka.binders.ingress.v1;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.statefun.flink.common.json.StateFunObjectMapper;
import org.junit.jupiter.api.Test;

/**
 * Pins the two-level {@code forwardHeaders} resolution contract of {@link
 * RoutableKafkaIngressSpec}: header forwarding is opt-in (default off), an ingress-level value
 * applies to all topics, and a per-topic value overrides the ingress-level one in either
 * direction.
 */
class RoutableKafkaIngressSpecTest {

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final ObjectMapper SPEC_MAPPER = StateFunObjectMapper.create();

  @Test
  void headerForwardingIsOffByDefault() throws Exception {
    RoutableKafkaIngressSpec spec = parseSpec(specYaml("", "", ""));

    assertThat(spec.forwardHeaderTopics()).isEmpty();
  }

  @Test
  void ingressLevelForwardHeadersAppliesToAllTopics() throws Exception {
    RoutableKafkaIngressSpec spec = parseSpec(specYaml("forwardHeaders: true", "", ""));

    assertThat(spec.forwardHeaderTopics()).containsExactlyInAnyOrder("topic-1", "topic-2");
  }

  @Test
  void perTopicOverrideDisablesForwardingDespiteIngressDefault() throws Exception {
    RoutableKafkaIngressSpec spec =
        parseSpec(specYaml("forwardHeaders: true", "forwardHeaders: false", ""));

    assertThat(spec.forwardHeaderTopics()).containsExactly("topic-2");
  }

  @Test
  void perTopicOverrideEnablesForwardingForThatTopicOnly() throws Exception {
    RoutableKafkaIngressSpec spec = parseSpec(specYaml("", "", "forwardHeaders: true"));

    assertThat(spec.forwardHeaderTopics()).containsExactly("topic-2");
  }

  private static RoutableKafkaIngressSpec parseSpec(String yaml) throws Exception {
    JsonNode specNode = YAML_MAPPER.readTree(yaml);
    return SPEC_MAPPER.treeToValue(specNode, RoutableKafkaIngressSpec.class);
  }

  private static String specYaml(
      String ingressLevelLine, String topic1Line, String topic2Line) {
    return String.join(
        "\n",
        "id: com.foo.bar/test-ingress",
        "address: kafka-broker:9092",
        ingressLevelLine,
        "topics:",
        "  - topic: topic-1",
        "    valueType: com.googleapis/com.mycomp.foo.MessageA",
        "    " + topic1Line,
        "    targets:",
        "      - com.mycomp.foo/function-1",
        "  - topic: topic-2",
        "    valueType: com.googleapis/com.mycomp.foo.MessageB",
        "    " + topic2Line,
        "    targets:",
        "      - com.mycomp.foo/function-2");
  }
}
