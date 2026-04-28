// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.jsonmodule;

import static org.apache.flink.statefun.flink.core.spi.ExtensionResolverAccessor.getExtensionResolver;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.extensions.ComponentBinder;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.flink.core.spi.ExtensionResolver;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

public final class RemoteModule implements StatefulFunctionModule {

  private final List<JsonNode> componentNodes;

  RemoteModule(List<JsonNode> componentNodes) {
    this.componentNodes = Objects.requireNonNull(componentNodes);
  }

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder moduleBinder) {
    parseComponentNodes(componentNodes)
        .forEach(component -> bindComponent(component, moduleBinder));
  }

  private static List<ComponentJsonObject> parseComponentNodes(
      Iterable<? extends JsonNode> componentNodes) {
    return StreamSupport.stream(componentNodes.spliterator(), false)
        .filter(node -> !node.isNull())
        .map(ComponentJsonObject::new)
        .collect(Collectors.toList());
  }

  private static void bindComponent(ComponentJsonObject component, Binder moduleBinder) {
    final ExtensionResolver extensionResolver = getExtensionResolver(moduleBinder);
    final ComponentBinder componentBinder =
        extensionResolver.resolveExtension(component.binderTypename(), ComponentBinder.class);
    componentBinder.bind(component, moduleBinder);
  }
}
