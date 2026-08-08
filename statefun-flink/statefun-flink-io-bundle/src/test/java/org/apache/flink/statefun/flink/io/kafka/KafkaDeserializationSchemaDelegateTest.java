// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.io.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.statefun.flink.common.UnimplementedTypeInfo;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class KafkaDeserializationSchemaDelegateTest {

  private static final class Utf8Deserializer implements KafkaIngressDeserializer<String> {
    private static final long serialVersionUID = 1L;

    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> record) {
      return new String(record.value(), StandardCharsets.UTF_8);
    }
  }

  private static final class ListCollector<T> implements Collector<T> {
    final List<T> collected = new ArrayList<>();

    @Override
    public void collect(T record) {
      collected.add(record);
    }

    @Override
    public void close() {}
  }

  private static final class NullOnTombstoneDeserializer implements KafkaIngressDeserializer<String> {
    private static final long serialVersionUID = 1L;

    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> record) {
      return record.value() == null ? null : new String(record.value(), StandardCharsets.UTF_8);
    }
  }

  private static final class RecordingMetrics {
    final Map<String, Counter> counters = new HashMap<>();

    MetricGroup group() {
      return group("");
    }

    private MetricGroup group(String scope) {
      return (MetricGroup) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {MetricGroup.class}, (proxy, method, args) -> {
        if (method.getName().equals("counter") && args != null && args.length == 1) {
          return counters.computeIfAbsent(scope + args[0], k -> new SimpleCounter());
        }
        if (method.getName().equals("addGroup") && args != null && args.length == 2) {
          return group(scope + args[0] + "." + args[1] + ".");
        }
        return null;
      });
    }

    long count(String name) {
      Counter counter = counters.get(name);
      return counter == null ? 0 : counter.getCount();
    }
  }

  private static DeserializationSchema.InitializationContext contextOf(RecordingMetrics metrics) {
    return new DeserializationSchema.InitializationContext() {
      @Override
      public MetricGroup getMetricGroup() {
        return metrics.group();
      }

      @Override
      public UserCodeClassLoader getUserCodeClassLoader() {
        return null;
      }
    };
  }

  @Test
  void nullFromDeserializerIsSkippedNotCollected() throws IOException {
    KafkaDeserializationSchemaDelegate<String> delegate = new KafkaDeserializationSchemaDelegate<>(new NullOnTombstoneDeserializer());

    ListCollector<String> collector = new ListCollector<>();
    delegate.deserialize(new ConsumerRecord<>("t", 0, 0L, "k".getBytes(StandardCharsets.UTF_8), null), collector);
    delegate.deserialize(new ConsumerRecord<>("t", 0, 1L, "k".getBytes(StandardCharsets.UTF_8), "ok".getBytes(StandardCharsets.UTF_8)), collector);

    assertThat(collector.collected).containsExactly("ok");
  }

  @Test
  void skippedRecordsAreCountedGloballyAndPerTopic() throws Exception {
    KafkaDeserializationSchemaDelegate<String> delegate = new KafkaDeserializationSchemaDelegate<>(new NullOnTombstoneDeserializer());
    RecordingMetrics metrics = new RecordingMetrics();
    delegate.open(contextOf(metrics));

    ListCollector<String> collector = new ListCollector<>();
    delegate.deserialize(new ConsumerRecord<>("orders", 0, 0L, "k".getBytes(StandardCharsets.UTF_8), null), collector);
    delegate.deserialize(new ConsumerRecord<>("orders", 0, 1L, "k".getBytes(StandardCharsets.UTF_8), null), collector);
    delegate.deserialize(new ConsumerRecord<>("payments", 0, 0L, "k".getBytes(StandardCharsets.UTF_8), null), collector);
    delegate.deserialize(new ConsumerRecord<>("orders", 0, 2L, "k".getBytes(StandardCharsets.UTF_8), "ok".getBytes(StandardCharsets.UTF_8)), collector);

    assertThat(metrics.count("numInvalidRecordsSkipped")).isEqualTo(3);
    assertThat(metrics.count("numRecordsInErrors")).isEqualTo(3);
    assertThat(metrics.count("topic.orders.numInvalidRecordsSkipped")).isEqualTo(2);
    assertThat(metrics.count("topic.payments.numInvalidRecordsSkipped")).isEqualTo(1);
    assertThat(collector.collected).containsExactly("ok");
  }

  @Test
  void skipCountingWithoutOpenIsANoOp() throws IOException {
    KafkaDeserializationSchemaDelegate<String> delegate = new KafkaDeserializationSchemaDelegate<>(new NullOnTombstoneDeserializer());

    ListCollector<String> collector = new ListCollector<>();
    delegate.deserialize(new ConsumerRecord<>("t", 0, 0L, "k".getBytes(StandardCharsets.UTF_8), null), collector);

    assertThat(collector.collected).isEmpty();
  }

  @Test
  void delegatesBytesToUserDeserializer() throws IOException {
    KafkaDeserializationSchemaDelegate<String> delegate =
        new KafkaDeserializationSchemaDelegate<>(new Utf8Deserializer());

    ConsumerRecord<byte[], byte[]> record =
        new ConsumerRecord<>(
            "t", 0, 0L, null, "payload".getBytes(StandardCharsets.UTF_8));

    ListCollector<String> collector = new ListCollector<>();
    delegate.deserialize(record, collector);

    assertThat(collector.collected).containsExactly("payload");
  }

  @Test
  void producedTypeReturnsUnimplementedPlaceholder() {
    KafkaDeserializationSchemaDelegate<String> delegate =
        new KafkaDeserializationSchemaDelegate<>(new Utf8Deserializer());

    assertThat(delegate.getProducedType()).isInstanceOf(UnimplementedTypeInfo.class);
  }

  @Test
  void rejectsNullDeserializer() {
    assertThatThrownBy(() -> new KafkaDeserializationSchemaDelegate<>(null))
        .isInstanceOf(NullPointerException.class);
  }
}
