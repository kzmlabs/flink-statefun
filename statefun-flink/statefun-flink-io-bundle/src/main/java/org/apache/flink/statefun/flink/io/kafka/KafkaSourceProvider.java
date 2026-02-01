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
package org.apache.flink.statefun.flink.io.kafka;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressStartupPosition;
import org.apache.flink.statefun.sdk.kafka.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

public class KafkaSourceProvider implements SourceProvider {

  @Override
  public <T, SplitT extends SourceSplit, EnumChckT> Source<T, SplitT, EnumChckT> forSpec(
      IngressSpec<T> ingressSpec) {
    KafkaIngressSpec<T> spec = asKafkaSpec(ingressSpec);

    KafkaSource<T> source =
        KafkaSource.<T>builder()
            .setBootstrapServers(
                spec.properties().getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))
            .setGroupId(spec.properties().getProperty(ConsumerConfig.GROUP_ID_CONFIG))
            .setTopics(spec.topics())
            .setDeserializer(deserializationSchemaFromSpec(spec))
            .setStartingOffsets(getStartupPositionFromSpec(spec))
            .setProperties(spec.properties())
            .build();

    //noinspection unchecked
    return (Source<T, SplitT, EnumChckT>) source;
  }

  private static <T> KafkaIngressSpec<T> asKafkaSpec(IngressSpec<T> ingressSpec) {
    if (ingressSpec instanceof KafkaIngressSpec) {
      return (KafkaIngressSpec<T>) ingressSpec;
    }
    if (ingressSpec == null) {
      throw new NullPointerException("Unable to translate a NULL spec");
    }
    throw new IllegalArgumentException(String.format("Wrong type %s", ingressSpec.type()));
  }

  private <T> OffsetsInitializer getStartupPositionFromSpec(KafkaIngressSpec<T> spec) {
    KafkaIngressStartupPosition startupPosition = spec.startupPosition();

    if (startupPosition.isGroupOffsets()) {
      return OffsetsInitializer.committedOffsets();
    } else if (startupPosition.isEarliest()) {
      return OffsetsInitializer.earliest();
    } else if (startupPosition.isLatest()) {
      return OffsetsInitializer.latest();
    } else if (startupPosition.isSpecificOffsets()) {
      KafkaIngressStartupPosition.SpecificOffsetsPosition offsetsPosition =
          startupPosition.asSpecificOffsets();
      return OffsetsInitializer.offsets(
          convertKafkaTopicPartitionMap(offsetsPosition.specificOffsets()));
    } else if (startupPosition.isDate()) {
      return OffsetsInitializer.timestamp(startupPosition.asDate().epochMilli());
    } else {
      throw new IllegalStateException("Safe guard; should not occur");
    }
  }

  private <T> KafkaRecordDeserializationSchema<T> deserializationSchemaFromSpec(
      KafkaIngressSpec<T> spec) {
    return new KafkaDeserializationSchemaDelegate<>(spec.deserializer());
  }

  private static Map<TopicPartition, Long> convertKafkaTopicPartitionMap(
      Map<KafkaTopicPartition, Long> offsets) {
    Map<TopicPartition, Long> result = new HashMap<>(offsets.size());
    for (Map.Entry<KafkaTopicPartition, Long> offset : offsets.entrySet()) {
      result.put(convertKafkaTopicPartition(offset.getKey()), offset.getValue());
    }
    return result;
  }

  private static TopicPartition convertKafkaTopicPartition(KafkaTopicPartition partition) {
    return new TopicPartition(partition.topic(), partition.partition());
  }
}
