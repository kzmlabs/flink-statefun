// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.httpfn;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.FunctionTypeNamespaceMatcher;
import org.apache.flink.statefun.sdk.TypeName;

public abstract class TargetFunctions implements Serializable {

  public static TargetFunctions fromPatternString(String patternString) {
    TypeName targetTypeName = TypeName.parseFrom(patternString);
    if (targetTypeName.namespace().contains("*")) {
      throw new IllegalArgumentException(
          "Invalid syntax for target functions. Only <namespace>/<name> or <namespace>/* are supported.");
    }
    if (targetTypeName.name().equals("*")) {
      return TargetFunctions.namespace(targetTypeName.namespace());
    }
    if (targetTypeName.name().contains("*")) {
      throw new IllegalArgumentException(
          "Invalid syntax for target functions. Only <namespace>/<name> or <namespace>/* are supported.");
    }
    final FunctionType functionType =
        new FunctionType(targetTypeName.namespace(), targetTypeName.name());
    return TargetFunctions.functionType(functionType);
  }

  public static TargetFunctions namespace(String namespace) {
    return new TargetFunctions.NamespaceTarget(
        FunctionTypeNamespaceMatcher.targetNamespace(namespace));
  }

  public static TargetFunctions functionType(FunctionType functionType) {
    return new TargetFunctions.FunctionTypeTarget(functionType);
  }

  public boolean isSpecificFunctionType() {
    return this.getClass() == TargetFunctions.FunctionTypeTarget.class;
  }

  public boolean isNamespace() {
    return this.getClass() == TargetFunctions.NamespaceTarget.class;
  }

  public abstract FunctionTypeNamespaceMatcher asNamespace();

  public abstract FunctionType asSpecificFunctionType();

  private static class NamespaceTarget extends TargetFunctions {
    private static final long serialVersionUID = 1;

    private final FunctionTypeNamespaceMatcher namespaceMatcher;

    private NamespaceTarget(FunctionTypeNamespaceMatcher namespaceMatcher) {
      this.namespaceMatcher = Objects.requireNonNull(namespaceMatcher);
    }

    @Override
    public FunctionTypeNamespaceMatcher asNamespace() {
      return namespaceMatcher;
    }

    @Override
    public FunctionType asSpecificFunctionType() {
      throw new IllegalStateException("This target is not a specific function type");
    }
  }

  private static class FunctionTypeTarget extends TargetFunctions {
    private static final long serialVersionUID = 1;

    private final FunctionType functionType;

    private FunctionTypeTarget(FunctionType functionType) {
      this.functionType = Objects.requireNonNull(functionType);
    }

    @Override
    public FunctionTypeNamespaceMatcher asNamespace() {
      throw new IllegalStateException("This target is not a namespace.");
    }

    @Override
    public FunctionType asSpecificFunctionType() {
      return functionType;
    }
  }
}
