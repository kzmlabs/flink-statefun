// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.datastream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.time.Duration;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.jupiter.api.Test;

class StatefulFunctionDataStreamBuilderTest {

  private static final FunctionType FN_A = new FunctionType("ns", "a");
  private static final FunctionType FN_B = new FunctionType("ns", "b");
  private static final EgressIdentifier<String> OUT_A =
      new EgressIdentifier<>("ns", "out-a", String.class);
  private static final EgressIdentifier<String> OUT_B =
      new EgressIdentifier<>("ns", "out-b", String.class);

  @Test
  void builderRejectsNullPipelineName() {
    assertThatThrownBy(() -> StatefulFunctionDataStreamBuilder.builder(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void builderReturnsFluentInstance() {
    StatefulFunctionDataStreamBuilder b = StatefulFunctionDataStreamBuilder.builder("p");
    assertThat(b).isNotNull();
  }

  @Test
  void withFunctionProviderRejectsNulls() {
    StatefulFunctionDataStreamBuilder b = StatefulFunctionDataStreamBuilder.builder("p");

    assertThatThrownBy(() -> b.withFunctionProvider(null, noopProvider()))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> b.withFunctionProvider(FN_A, null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void withFunctionProviderRejectsDuplicateRegistrationOfSameType() {
    StatefulFunctionDataStreamBuilder b = StatefulFunctionDataStreamBuilder.builder("p");
    b.withFunctionProvider(FN_A, noopProvider());

    assertThatThrownBy(() -> b.withFunctionProvider(FN_A, noopProvider()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("previously defined");
  }

  @Test
  void withFunctionProviderAcceptsDifferentTypes() {
    StatefulFunctionDataStreamBuilder b = StatefulFunctionDataStreamBuilder.builder("p");
    b.withFunctionProvider(FN_A, noopProvider());

    assertThatCode(() -> b.withFunctionProvider(FN_B, noopProvider()))
        .doesNotThrowAnyException();
  }

  @Test
  void withDataStreamAsIngressRejectsNull() {
    StatefulFunctionDataStreamBuilder b = StatefulFunctionDataStreamBuilder.builder("p");

    assertThatThrownBy(() -> b.withDataStreamAsIngress((DataStream<RoutableMessage>) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void withEgressIdRejectsNullAndDuplicates() {
    StatefulFunctionDataStreamBuilder b = StatefulFunctionDataStreamBuilder.builder("p");

    assertThatThrownBy(() -> b.withEgressId(null)).isInstanceOf(NullPointerException.class);
    b.withEgressId(OUT_A);
    assertThatThrownBy(() -> b.withEgressId(OUT_A))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("previously defined");
    assertThatCode(() -> b.withEgressId(OUT_B)).doesNotThrowAnyException();
  }

  @Test
  void withRequestReplyRemoteFunctionRejectsNull() {
    StatefulFunctionDataStreamBuilder b = StatefulFunctionDataStreamBuilder.builder("p");

    assertThatThrownBy(() -> b.withRequestReplyRemoteFunction(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void withRequestReplyRemoteFunctionRejectsDuplicateType() throws Exception {
    StatefulFunctionDataStreamBuilder b = StatefulFunctionDataStreamBuilder.builder("p");

    StatefulFunctionBuilder rr1 = newRemoteBuilder(FN_A);
    StatefulFunctionBuilder rr2 = newRemoteBuilder(FN_A);
    b.withRequestReplyRemoteFunction(rr1);

    assertThatThrownBy(() -> b.withRequestReplyRemoteFunction(rr2))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("previously defined");
  }

  @Test
  void withConfigurationRejectsNull() {
    StatefulFunctionDataStreamBuilder b = StatefulFunctionDataStreamBuilder.builder("p");

    assertThatThrownBy(() -> b.withConfiguration(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void withConfigurationAcceptsAValidConfig() {
    StatefulFunctionDataStreamBuilder b = StatefulFunctionDataStreamBuilder.builder("p");
    StatefulFunctionsConfig cfg =
        StatefulFunctionsConfig.fromFlinkConfiguration(new org.apache.flink.configuration.Configuration());
    cfg.setFactoryType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS);

    assertThatCode(() -> b.withConfiguration(cfg)).doesNotThrowAnyException();
  }

  private static SerializableStatefulFunctionProvider noopProvider() {
    return functionType ->
        new StatefulFunction() {
          @Override
          public void invoke(Context context, Object input) {}
        };
  }

  private static StatefulFunctionBuilder newRemoteBuilder(FunctionType type) throws Exception {
    // Default connect timeout is large enough that we must explicitly bound the request
    // duration above it; otherwise the builder rejects the spec.
    return RequestReplyFunctionBuilder.requestReplyFunctionBuilder(type, new URI("http://localhost:1234/"))
        .withMaxNumBatchRequests(10)
        .withConnectTimeout(Duration.ofSeconds(2))
        .withMaxRequestDuration(Duration.ofSeconds(30));
  }

  // Address used solely as a placeholder where API surfaces require it.
  @SuppressWarnings("unused")
  private static final Address SAMPLE_ADDR = new Address(FN_A, "id");
}
