// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.httpfn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.statefun.sdk.FunctionType;
import org.junit.jupiter.api.Test;

/**
 * Pin the {@link TargetFunctions#fromPatternString} contract: only {@code <namespace>/<name>} or
 * {@code <namespace>/*} are accepted. Comma-lists, wildcards in the namespace, and partial
 * wildcards in the name are all rejected with {@link IllegalArgumentException}. This is a
 * regression vector — module.yaml authors regularly try patterns like {@code counter.*\/*} or
 * {@code "counter/a, counter/b"} and silently drop bindings if validation slips.
 */
class TargetFunctionsTest {

  @Test
  void exactNamespaceAndNameProducesSpecificFunctionTypeTarget() {
    TargetFunctions target = TargetFunctions.fromPatternString("counter/increment");

    assertThat(target.isSpecificFunctionType()).isTrue();
    assertThat(target.isNamespace()).isFalse();
    assertThat(target.asSpecificFunctionType())
        .isEqualTo(new FunctionType("counter", "increment"));
  }

  @Test
  void wildcardNameProducesNamespaceTarget() {
    TargetFunctions target = TargetFunctions.fromPatternString("counter/*");

    assertThat(target.isNamespace()).isTrue();
    assertThat(target.isSpecificFunctionType()).isFalse();
    assertThat(target.asNamespace().targetNamespace()).isEqualTo("counter");
  }

  @Test
  void wildcardInNamespaceIsRejected() {
    assertThatThrownBy(() -> TargetFunctions.fromPatternString("counter.*/foo"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Only <namespace>/<name> or <namespace>/* are supported");
  }

  @Test
  void wildcardOnBothSidesIsRejected() {
    assertThatThrownBy(() -> TargetFunctions.fromPatternString("*/*"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void partialWildcardInNameIsRejected() {
    assertThatThrownBy(() -> TargetFunctions.fromPatternString("counter/inc*"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Only <namespace>/<name> or <namespace>/* are supported");
  }

  @Test
  void asNamespaceOnSpecificFunctionTypeThrows() {
    TargetFunctions target = TargetFunctions.fromPatternString("counter/increment");

    assertThatThrownBy(target::asNamespace)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not a namespace");
  }

  @Test
  void asSpecificFunctionTypeOnNamespaceTargetThrows() {
    TargetFunctions target = TargetFunctions.fromPatternString("counter/*");

    assertThatThrownBy(target::asSpecificFunctionType)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not a specific function type");
  }

  @Test
  void factoryNamespaceProducesNamespaceTarget() {
    TargetFunctions target = TargetFunctions.namespace("payments");

    assertThat(target.isNamespace()).isTrue();
    assertThat(target.asNamespace().targetNamespace()).isEqualTo("payments");
  }

  @Test
  void factoryFunctionTypeProducesSpecificTarget() {
    FunctionType ft = new FunctionType("payments", "process");
    TargetFunctions target = TargetFunctions.functionType(ft);

    assertThat(target.isSpecificFunctionType()).isTrue();
    assertThat(target.asSpecificFunctionType()).isEqualTo(ft);
  }

  @Test
  void factoryNamespaceRejectsNullNamespace() {
    // FunctionTypeNamespaceMatcher.targetNamespace requires non-null; surface that.
    assertThatThrownBy(() -> TargetFunctions.namespace(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void factoryFunctionTypeRejectsNullFunctionType() {
    assertThatThrownBy(() -> TargetFunctions.functionType(null))
        .isInstanceOf(NullPointerException.class);
  }
}
