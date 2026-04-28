// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.io.testutils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.FunctionTypeNamespaceMatcher;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

public final class TestModuleBinder implements StatefulFunctionModule.Binder {
  private final Map<IngressIdentifier<?>, IngressSpec<?>> ingress = new HashMap<>();
  private final Map<EgressIdentifier<?>, EgressSpec<?>> egress = new HashMap<>();
  private final Map<IngressIdentifier<?>, List<Router<?>>> routers = new HashMap<>();
  private final Map<FunctionType, StatefulFunctionProvider> specificFunctionProviders =
      new HashMap<>();
  private final Map<String, StatefulFunctionProvider> namespaceFunctionProviders = new HashMap<>();

  @Override
  public <T> void bindIngress(IngressSpec<T> spec) {
    Objects.requireNonNull(spec);
    IngressIdentifier<T> id = spec.id();
    ingress.put(id, spec);
  }

  @Override
  public <T> void bindIngressRouter(IngressIdentifier<T> ingressIdentifier, Router<T> router) {
    Objects.requireNonNull(ingressIdentifier);
    Objects.requireNonNull(router);

    List<Router<?>> ingressRouters =
        routers.computeIfAbsent(ingressIdentifier, unused -> new ArrayList<>());
    ingressRouters.add(router);
  }

  @Override
  public <T> void bindEgress(EgressSpec<T> spec) {
    Objects.requireNonNull(spec);
    EgressIdentifier<T> id = spec.id();
    egress.put(id, spec);
  }

  @Override
  public void bindFunctionProvider(FunctionType functionType, StatefulFunctionProvider provider) {
    Objects.requireNonNull(functionType);
    Objects.requireNonNull(provider);
    specificFunctionProviders.put(functionType, provider);
  }

  @Override
  public void bindFunctionProvider(
      FunctionTypeNamespaceMatcher namespaceMatcher, StatefulFunctionProvider provider) {
    Objects.requireNonNull(namespaceMatcher);
    Objects.requireNonNull(provider);
    namespaceFunctionProviders.put(namespaceMatcher.targetNamespace(), provider);
  }

  @SuppressWarnings("unchecked")
  public <T> IngressSpec<T> getIngress(IngressIdentifier<T> ingressIdentifier) {
    return (IngressSpec<T>) ingress.get(ingressIdentifier);
  }

  public <T> List<Router<?>> getRouters(IngressIdentifier<T> ingressIdentifier) {
    return routers.get(ingressIdentifier);
  }

  @SuppressWarnings("unchecked")
  public <T> EgressSpec<T> getEgress(EgressIdentifier<T> egressIdentifier) {
    return (EgressSpec<T>) egress.get(egressIdentifier);
  }
}
