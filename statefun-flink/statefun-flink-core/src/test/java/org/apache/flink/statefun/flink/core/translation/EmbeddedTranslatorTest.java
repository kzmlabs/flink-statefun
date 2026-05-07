// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.translation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverseProvider;
import org.apache.flink.statefun.flink.core.feedback.FeedbackKey;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.junit.jupiter.api.Test;

/**
 * Pins {@link EmbeddedTranslator} state-rebuild path. Full translate() runs through Flink's
 * StreamExecutionEnvironment so it's covered by the e2e suite — these tests only exercise the
 * embedded universe provider plumbing that the translator wires in.
 */
class EmbeddedTranslatorTest {

  @Test
  void translatorIsConstructible() {
    StatefulFunctionsConfig cfg = newConfig();
    FeedbackKey<Message> fk = new FeedbackKey<>("p", 1);

    EmbeddedTranslator translator = new EmbeddedTranslator(cfg, fk);

    assertThat(translator).isNotNull();
  }

  @Test
  void embeddedUniverseProviderRegistersAllFunctionProviders() throws Exception {
    StatefulFunctionsConfig cfg = newConfig();
    FeedbackKey<Message> fk = new FeedbackKey<>("p", 1);
    new EmbeddedTranslator(cfg, fk);

    Map<FunctionType, SerializableNoopProvider> functions = new HashMap<>();
    FunctionType ft1 = new FunctionType("ns", "fn-a");
    FunctionType ft2 = new FunctionType("ns", "fn-b");
    functions.put(ft1, new SerializableNoopProvider());
    functions.put(ft2, new SerializableNoopProvider());

    StatefulFunctionsUniverseProvider provider = newProviderViaConfig(cfg, functions);
    StatefulFunctionsUniverse universe = provider.get(getClass().getClassLoader(), cfg);

    assertThat(universe.functions()).containsKeys(ft1, ft2);
    assertThat(universe.functions()).hasSize(2);
  }

  @Test
  void embeddedUniverseProviderIsJavaSerializable() throws Exception {
    StatefulFunctionsConfig cfg = newConfig();
    Map<FunctionType, SerializableNoopProvider> functions = new HashMap<>();
    functions.put(new FunctionType("ns", "fn"), new SerializableNoopProvider());

    StatefulFunctionsUniverseProvider provider = newProviderViaConfig(cfg, functions);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    new ObjectOutputStream(out).writeObject(provider);
    Object copy =
        new ObjectInputStream(new ByteArrayInputStream(out.toByteArray())).readObject();

    assertThat(copy).isInstanceOf(StatefulFunctionsUniverseProvider.class);
  }

  @Test
  void embeddedUniverseProviderRejectsNullFunctionMap() {
    // The provider eagerly null-checks the function map in its constructor — pin that contract
    // so a future refactor doesn't defer the failure to JobGraph submission time, where the
    // operator would see only a confusing classpath-deserialization error.
    assertThatThrownBy(() -> buildProvider(null))
        .isInstanceOf(java.lang.reflect.InvocationTargetException.class)
        .hasCauseInstanceOf(NullPointerException.class);
  }

  private static StatefulFunctionsConfig newConfig() {
    StatefulFunctionsConfig cfg =
        StatefulFunctionsConfig.fromFlinkConfiguration(new org.apache.flink.configuration.Configuration());
    cfg.setFactoryType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS);
    return cfg;
  }

  /**
   * The translator's internal {@code EmbeddedUniverseProvider} is package-private nested in
   * {@link EmbeddedTranslator}. We build one via reflection to exercise its
   * {@link StatefulFunctionsUniverseProvider#get} contract.
   */
  private static StatefulFunctionsUniverseProvider newProviderViaConfig(
      StatefulFunctionsConfig cfg, Map<FunctionType, ? extends Serializable> functions)
      throws Exception {
    StatefulFunctionsUniverseProvider provider = buildProvider(functions);
    cfg.setProvider(provider);
    return provider;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static StatefulFunctionsUniverseProvider buildProvider(Map<FunctionType, ?> functions)
      throws Exception {
    Class<?> embedded =
        Class.forName(
            "org.apache.flink.statefun.flink.core.translation.EmbeddedTranslator$EmbeddedUniverseProvider");
    java.lang.reflect.Constructor<?> ctor = embedded.getDeclaredConstructor(Map.class);
    ctor.setAccessible(true);
    return (StatefulFunctionsUniverseProvider) ctor.newInstance(functions);
  }

  private static final class SerializableNoopProvider
      implements StatefulFunctionProvider, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public StatefulFunction functionOfType(FunctionType type) {
      return new StatefulFunction() {
        @Override
        public void invoke(Context context, Object input) {}
      };
    }
  }

  // Reference field to avoid unused-import warnings for Address — kept for symmetry with other
  // tests in this package.
  @SuppressWarnings("unused")
  private static final Address SAMPLE = new Address(new FunctionType("ns", "fn"), "id");
}
