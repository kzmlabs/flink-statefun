// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.sdk.java;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;
import org.junit.jupiter.api.Test;

class StatefulFunctionsTest {

  private static final TypeName FN_A = TypeName.typeNameOf("ns", "fn-a");
  private static final TypeName FN_B = TypeName.typeNameOf("ns", "fn-b");

  @Test
  void registerSingleSpecExposedThroughFunctionSpecsMap() {
    StatefulFunctions registry = new StatefulFunctions();
    StatefulFunctionSpec spec =
        StatefulFunctionSpec.builder(FN_A).withSupplier(EmptyFn::new).build();

    registry.withStatefulFunction(spec);

    assertThat(registry.functionSpecs()).containsEntry(FN_A, spec).hasSize(1);
  }

  @Test
  void registerSpecBuilderBuildsBeforeStorage() {
    StatefulFunctions registry = new StatefulFunctions();

    registry.withStatefulFunction(StatefulFunctionSpec.builder(FN_A).withSupplier(EmptyFn::new));

    assertThat(registry.functionSpecs()).containsKey(FN_A);
  }

  @Test
  void registerRejectsDuplicateTypeName() {
    StatefulFunctions registry = new StatefulFunctions();
    registry.withStatefulFunction(
        StatefulFunctionSpec.builder(FN_A).withSupplier(EmptyFn::new));

    assertThatThrownBy(
            () ->
                registry.withStatefulFunction(
                    StatefulFunctionSpec.builder(FN_A).withSupplier(EmptyFn::new)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("more than one")
        .hasMessageContaining(FN_A.toString());
  }

  @Test
  void multipleDistinctTypesCoexist() {
    StatefulFunctions registry = new StatefulFunctions();
    registry.withStatefulFunction(
        StatefulFunctionSpec.builder(FN_A).withSupplier(EmptyFn::new));
    registry.withStatefulFunction(
        StatefulFunctionSpec.builder(FN_B).withSupplier(EmptyFn::new));

    assertThat(registry.functionSpecs()).containsOnlyKeys(FN_A, FN_B);
  }

  @Test
  void requestReplyHandlerIsConstructibleWithoutRegistrations() {
    StatefulFunctions registry = new StatefulFunctions();

    RequestReplyHandler handler = registry.requestReplyHandler();

    assertThat(handler).isNotNull();
  }

  @Test
  void chainedRegistrationReturnsSameRegistry() {
    StatefulFunctions registry = new StatefulFunctions();

    StatefulFunctions afterFirst =
        registry.withStatefulFunction(
            StatefulFunctionSpec.builder(FN_A).withSupplier(EmptyFn::new));
    StatefulFunctions afterSecond =
        afterFirst.withStatefulFunction(
            StatefulFunctionSpec.builder(FN_B).withSupplier(EmptyFn::new));

    // Fluent API contract — same instance, so chained calls share state.
    assertThat(afterFirst).isSameAs(registry);
    assertThat(afterSecond).isSameAs(registry);
  }

  /** A no-op StatefulFunction used as a supplier target in spec building. */
  static final class EmptyFn implements StatefulFunction {
    @Override
    public CompletableFuture<Void> apply(
        org.apache.flink.statefun.sdk.java.Context context,
        org.apache.flink.statefun.sdk.java.message.Message message) {
      return CompletableFuture.completedFuture(null);
    }
  }
}
