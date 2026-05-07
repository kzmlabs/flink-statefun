// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.jsonmodule;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.extensions.ComponentBinder;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.extensions.ExtensionModule;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.message.MessageFactoryKey;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.junit.jupiter.api.Test;

/**
 * Drives the {@link LegacyRemoteModuleV30} backwards-compat path end-to-end against a real {@code
 * version: "3.0"} fixture, exercising the legacy-kind translation table (http → endpoints.v1/http,
 * io.statefun.kafka/* → kafka.v1/*) and the endpoint/ingress/egress component reconstruction
 * (which the new format format-3.1 path doesn't go through).
 */
class LegacyRemoteModuleV30Test {

  private static final String LEGACY_MODULE_PATH = "module-v3_0/module.yaml";

  // Binder kinds the v3.0 fixture references. Two of them are exercised through the legacy
  // translation table; the third (statefun.kafka.io/protobuf-ingress) is not in the table, so the
  // module passes the literal kind through as-is.
  private static final TypeName HTTP_ENDPOINT = TypeName.parseFrom("io.statefun.endpoints.v1/http");
  private static final TypeName KAFKA_EGRESS_TRANSLATED =
      TypeName.parseFrom("io.statefun.kafka.v1/egress");
  private static final TypeName UNTRANSLATED_INGRESS =
      TypeName.parseFrom("statefun.kafka.io/protobuf-ingress");

  @Test
  void v3_0FixtureLoadsAndDispatchesLegacyEndpointBinder() {
    Recorder recorder = new Recorder();
    setupUniverse(recorder);

    // The fixture has one HTTP endpoint binding under the legacy "http" kind.
    assertThat(recorder.boundFunctions).containsKey(LegacyHttpBinder.FUNCTION_TYPE);
    assertThat(recorder.bindCounts).containsEntry(HTTP_ENDPOINT, 1);
  }

  @Test
  void v3_0FixtureTranslatesIoStatefunKafkaEgressKindToV1Namespace() {
    Recorder recorder = new Recorder();
    setupUniverse(recorder);

    // The fixture has `io.statefun.kafka/egress` which the legacy table maps to v1/egress.
    assertThat(recorder.bindCounts).containsEntry(KAFKA_EGRESS_TRANSLATED, 1);
    assertThat(recorder.boundEgresses).containsKey(LegacyKafkaEgressBinder.EGRESS_ID);
  }

  @Test
  void v3_0FixturePassesUnrecognizedKindThroughAsTypename() {
    Recorder recorder = new Recorder();
    setupUniverse(recorder);

    // The fixture's ingress kind `statefun.kafka.io/protobuf-ingress` is NOT in the legacy
    // translation table — `tryConvertLegacyBinderKindTypeName` falls through to TypeName.parseFrom.
    // Pin that unrecognized kinds are passed verbatim, not silently dropped.
    assertThat(recorder.bindCounts).containsEntry(UNTRANSLATED_INGRESS, 1);
    assertThat(recorder.boundIngresses).containsKey(LegacyKafkaIngressBinder.INGRESS_ID);
  }

  @Test
  void v3_0FixtureBindsAllThreeComponentsExactlyOnce() {
    Recorder recorder = new Recorder();
    setupUniverse(recorder);

    // Endpoints (1) + ingresses (1) + egresses (1) = 3 binder dispatches total.
    assertThat(recorder.bindCounts.values().stream().mapToInt(Integer::intValue).sum())
        .isEqualTo(3);
  }

  // ---------- harness ----------

  private static void setupUniverse(Recorder recorder) {
    URL moduleUrl =
        LegacyRemoteModuleV30Test.class.getClassLoader().getResource(LEGACY_MODULE_PATH);
    assertThat(moduleUrl).isNotNull();

    ObjectMapper mapper = JsonServiceLoader.mapper();
    StatefulFunctionModule legacyModule = JsonServiceLoader.fromUrl(mapper, moduleUrl);
    assertThat(legacyModule).isInstanceOf(LegacyRemoteModuleV30.class);

    StatefulFunctionsUniverse universe =
        new StatefulFunctionsUniverse(
            MessageFactoryKey.forType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, null));
    new LegacyBindersExtensionModule(recorder).configure(new HashMap<>(), universe);
    legacyModule.configure(new HashMap<>(), universe);
  }

  private static final class Recorder {
    final Map<TypeName, Integer> bindCounts = new HashMap<>();
    final Map<FunctionType, StatefulFunctionProvider> boundFunctions = new HashMap<>();
    final Map<IngressIdentifier<?>, IngressSpec<?>> boundIngresses = new HashMap<>();
    final Map<EgressIdentifier<?>, EgressSpec<?>> boundEgresses = new HashMap<>();

    void recordBind(TypeName binderTypename) {
      bindCounts.merge(binderTypename, 1, Integer::sum);
    }
  }

  private static final class LegacyBindersExtensionModule implements ExtensionModule {
    private final Recorder recorder;

    LegacyBindersExtensionModule(Recorder recorder) {
      this.recorder = recorder;
    }

    @Override
    public void configure(Map<String, String> globalConfigurations, Binder binder) {
      binder.bindExtension(HTTP_ENDPOINT, new LegacyHttpBinder(recorder));
      binder.bindExtension(UNTRANSLATED_INGRESS, new LegacyKafkaIngressBinder(recorder));
      binder.bindExtension(KAFKA_EGRESS_TRANSLATED, new LegacyKafkaEgressBinder(recorder));
    }
  }

  private static final class LegacyHttpBinder implements ComponentBinder {
    static final FunctionType FUNCTION_TYPE = new FunctionType("com.foo.bar", "any");

    private final Recorder recorder;

    LegacyHttpBinder(Recorder recorder) {
      this.recorder = recorder;
    }

    @Override
    public void bind(
        ComponentJsonObject component, StatefulFunctionModule.Binder remoteModuleBinder) {
      recorder.recordBind(HTTP_ENDPOINT);
      StatefulFunctionProvider provider = new NoopFunctionProvider();
      remoteModuleBinder.bindFunctionProvider(FUNCTION_TYPE, provider);
      recorder.boundFunctions.put(FUNCTION_TYPE, provider);
    }
  }

  private static final class LegacyKafkaIngressBinder implements ComponentBinder {
    static final IngressIdentifier<String> INGRESS_ID =
        new IngressIdentifier<>(String.class, "com.mycomp.igal", "names");

    private final Recorder recorder;

    LegacyKafkaIngressBinder(Recorder recorder) {
      this.recorder = recorder;
    }

    @Override
    public void bind(
        ComponentJsonObject component, StatefulFunctionModule.Binder remoteModuleBinder) {
      recorder.recordBind(UNTRANSLATED_INGRESS);
      IngressSpec<String> spec = new NoopIngressSpec(INGRESS_ID);
      remoteModuleBinder.bindIngress(spec);
      recorder.boundIngresses.put(INGRESS_ID, spec);
    }
  }

  private static final class LegacyKafkaEgressBinder implements ComponentBinder {
    static final EgressIdentifier<String> EGRESS_ID =
        new EgressIdentifier<>("com.mycomp.foo", "bar", String.class);

    private final Recorder recorder;

    LegacyKafkaEgressBinder(Recorder recorder) {
      this.recorder = recorder;
    }

    @Override
    public void bind(
        ComponentJsonObject component, StatefulFunctionModule.Binder remoteModuleBinder) {
      recorder.recordBind(KAFKA_EGRESS_TRANSLATED);
      EgressSpec<String> spec = new NoopEgressSpec(EGRESS_ID);
      remoteModuleBinder.bindEgress(spec);
      recorder.boundEgresses.put(EGRESS_ID, spec);
    }
  }

  private static final class NoopFunctionProvider implements StatefulFunctionProvider {
    @Override
    public StatefulFunction functionOfType(FunctionType type) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class NoopIngressSpec implements IngressSpec<String> {
    private final IngressIdentifier<String> id;

    NoopIngressSpec(IngressIdentifier<String> id) {
      this.id = id;
    }

    @Override
    public IngressIdentifier<String> id() {
      return id;
    }

    @Override
    public IngressType type() {
      throw new UnsupportedOperationException();
    }
  }

  private static final class NoopEgressSpec implements EgressSpec<String> {
    private final EgressIdentifier<String> id;

    NoopEgressSpec(EgressIdentifier<String> id) {
      this.id = id;
    }

    @Override
    public EgressIdentifier<String> id() {
      return id;
    }

    @Override
    public EgressType type() {
      throw new UnsupportedOperationException();
    }
  }
}
