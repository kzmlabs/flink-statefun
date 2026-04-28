// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.kafka;

import static org.apache.flink.statefun.sdk.kafka.testutils.Matchers.hasProperty;
import static org.apache.flink.statefun.sdk.kafka.testutils.Matchers.isMapOfSize;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.util.Properties;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

public class KafkaIngressBuilderTest {

  private static final IngressIdentifier<String> DUMMY_ID =
      new IngressIdentifier<>(String.class, "ns", "name");

  @Test
  public void idIsCorrect() {
    KafkaIngressBuilder<String> builder =
        KafkaIngressBuilder.forIdentifier(DUMMY_ID)
            .withKafkaAddress("localhost:8082")
            .withTopic("topic")
            .withConsumerGroupId("test-group")
            .withDeserializer(NoOpDeserializer.class);

    KafkaIngressSpec<String> spec = builder.build();

    assertThat(spec.id(), is(DUMMY_ID));
  }

  @Test
  public void ingressTypeIsCorrect() {
    KafkaIngressBuilder<String> builder =
        KafkaIngressBuilder.forIdentifier(DUMMY_ID)
            .withKafkaAddress("localhost:8082")
            .withTopic("topic")
            .withConsumerGroupId("test-group")
            .withDeserializer(NoOpDeserializer.class);

    KafkaIngressSpec<String> spec = builder.build();

    assertThat(spec.type(), is(Constants.KAFKA_INGRESS_TYPE));
  }

  @Test
  public void topicsIsCorrect() {
    KafkaIngressBuilder<String> builder =
        KafkaIngressBuilder.forIdentifier(DUMMY_ID)
            .withKafkaAddress("localhost:8082")
            .withTopic("topic")
            .withConsumerGroupId("test-group")
            .withDeserializer(NoOpDeserializer.class);

    KafkaIngressSpec<String> spec = builder.build();

    assertThat(spec.topics(), contains("topic"));
  }

  @Test
  public void deserializerIsCorrect() {
    KafkaIngressBuilder<String> builder =
        KafkaIngressBuilder.forIdentifier(DUMMY_ID)
            .withKafkaAddress("localhost:8082")
            .withTopic("topic")
            .withConsumerGroupId("test-group")
            .withDeserializer(NoOpDeserializer.class);

    KafkaIngressSpec<String> spec = builder.build();

    assertThat(spec.deserializer(), instanceOf(NoOpDeserializer.class));
  }

  @Test
  public void startupPositionIsCorrect() {
    KafkaIngressBuilder<String> builder =
        KafkaIngressBuilder.forIdentifier(DUMMY_ID)
            .withKafkaAddress("localhost:8082")
            .withTopic("topic")
            .withConsumerGroupId("test-group")
            .withDeserializer(NoOpDeserializer.class);

    KafkaIngressSpec<String> spec = builder.build();

    assertThat(spec.startupPosition(), is(KafkaIngressStartupPosition.fromLatest()));
  }

  @Test
  public void propertiesIsCorrect() {
    KafkaIngressBuilder<String> builder =
        KafkaIngressBuilder.forIdentifier(DUMMY_ID)
            .withKafkaAddress("localhost:8082")
            .withTopic("topic")
            .withConsumerGroupId("test-group")
            .withDeserializer(NoOpDeserializer.class);

    KafkaIngressSpec<String> spec = builder.build();

    assertThat(
        spec.properties(),
        allOf(
            isMapOfSize(3),
            hasProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8082"),
            hasProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group"),
            hasProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")));
  }

  @Test
  public void namedMethodConfigValuesOverwriteProperties() {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "should-be-overwritten");

    KafkaIngressBuilder<String> builder =
        KafkaIngressBuilder.forIdentifier(DUMMY_ID)
            .withKafkaAddress("localhost:8082")
            .withTopic("topic")
            .withConsumerGroupId("test-group")
            .withDeserializer(NoOpDeserializer.class)
            .withProperties(properties);

    KafkaIngressSpec<String> spec = builder.build();

    assertThat(
        spec.properties(), hasProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8082"));
  }

  @Test
  public void defaultNamedMethodConfigValuesShouldNotOverwriteProperties() {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KafkaIngressBuilder<String> builder =
        KafkaIngressBuilder.forIdentifier(DUMMY_ID)
            .withKafkaAddress("localhost:8082")
            .withTopic("topic")
            .withConsumerGroupId("test-group")
            .withDeserializer(NoOpDeserializer.class)
            .withProperties(properties);

    KafkaIngressSpec<String> spec = builder.build();

    assertThat(spec.properties(), hasProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
  }

  private static class NoOpDeserializer implements KafkaIngressDeserializer<String> {
    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> input) {
      return null;
    }
  }
}
