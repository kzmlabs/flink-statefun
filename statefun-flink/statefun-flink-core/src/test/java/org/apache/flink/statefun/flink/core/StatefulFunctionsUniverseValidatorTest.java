// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.statefun.flink.core.message.MessageFactoryKey;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.FunctionTypeNamespaceMatcher;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.io.Router;
import org.junit.jupiter.api.Test;

/**
 * Pins the four guard rails that {@link StatefulFunctionsUniverseValidator} enforces. The
 * validator runs once during job graph construction; each missing component must produce a
 * specific error so operators can fix their module.yaml without trial-and-error.
 */
class StatefulFunctionsUniverseValidatorTest {

  private final StatefulFunctionsUniverseValidator validator =
      new StatefulFunctionsUniverseValidator();

  @Test
  void rejectsUniverseWithNoIngress() {
    StatefulFunctionsUniverse u = newUniverse();
    bindSource(u);
    bindRouter(u);
    bindFunction(u);
    // No ingress bound.

    assertThatThrownBy(() -> validator.validate(u))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("no ingress");
  }

  @Test
  void rejectsUniverseWithIngressButNoSource() {
    StatefulFunctionsUniverse u = newUniverse();
    bindIngress(u);
    bindRouter(u);
    bindFunction(u);
    // No source provider bound.

    assertThatThrownBy(() -> validator.validate(u))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("source providers");
  }

  @Test
  void rejectsUniverseWithIngressAndSourceButNoRouter() {
    StatefulFunctionsUniverse u = newUniverse();
    bindIngress(u);
    bindSource(u);
    bindFunction(u);
    // No router bound.

    assertThatThrownBy(() -> validator.validate(u))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("routers");
  }

  @Test
  void rejectsUniverseWithoutAnyFunctionProviders() {
    StatefulFunctionsUniverse u = newUniverse();
    bindIngress(u);
    bindSource(u);
    bindRouter(u);
    // No specific or namespace function providers bound.

    assertThatThrownBy(() -> validator.validate(u))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("function providers");
  }

  @Test
  void acceptsUniverseWithSpecificFunctionProvider() {
    StatefulFunctionsUniverse u = newUniverse();
    bindIngress(u);
    bindSource(u);
    bindRouter(u);
    bindFunction(u);

    assertThatCode(() -> validator.validate(u)).doesNotThrowAnyException();
  }

  @Test
  void acceptsUniverseWithNamespaceFunctionProviderOnly() {
    StatefulFunctionsUniverse u = newUniverse();
    bindIngress(u);
    bindSource(u);
    bindRouter(u);
    // Only namespace match — no specific function provider.
    u.bindFunctionProvider(
        FunctionTypeNamespaceMatcher.targetNamespace("ns"), ignored -> null);

    assertThatCode(() -> validator.validate(u)).doesNotThrowAnyException();
  }

  @Test
  void egressAndSinkAreNotRequired() {
    // Validator currently does not require egress/sink — pin that contract.
    StatefulFunctionsUniverse u = newUniverse();
    bindIngress(u);
    bindSource(u);
    bindRouter(u);
    bindFunction(u);

    assertThatCode(() -> validator.validate(u)).doesNotThrowAnyException();
  }

  private static StatefulFunctionsUniverse newUniverse() {
    return new StatefulFunctionsUniverse(
        MessageFactoryKey.forType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, null));
  }

  private static void bindIngress(StatefulFunctionsUniverse u) {
    IngressIdentifier<String> id = new IngressIdentifier<>(String.class, "ns", "in");
    u.bindIngress(
        new IngressSpec<String>() {
          @Override
          public IngressIdentifier<String> id() {
            return id;
          }

          @Override
          public IngressType type() {
            return new IngressType("ns", "kind");
          }
        });
  }

  private static void bindRouter(StatefulFunctionsUniverse u) {
    IngressIdentifier<String> id = new IngressIdentifier<>(String.class, "ns", "in");
    u.bindIngressRouter(id, (msg, sink) -> {});
  }

  private static void bindSource(StatefulFunctionsUniverse u) {
    u.bindSourceProvider(
        new IngressType("ns", "kind"),
        new SourceProvider() {
          @Override
          public <T, SplitT extends org.apache.flink.api.connector.source.SourceSplit, EnumChckT>
              org.apache.flink.api.connector.source.Source<T, SplitT, EnumChckT> forSpec(
                  org.apache.flink.statefun.sdk.io.IngressSpec<T> spec) {
            return null;
          }
        });
  }

  private static void bindFunction(StatefulFunctionsUniverse u) {
    u.bindFunctionProvider(
        new FunctionType("ns", "fn"), (StatefulFunctionProvider) ignored -> null);
  }
}
