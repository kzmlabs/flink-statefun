// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.httpfn.binders.v2;

import static org.apache.flink.statefun.flink.core.spi.ExtensionResolverAccessor.getExtensionResolver;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.extensions.ComponentBinder;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.flink.common.json.StateFunObjectMapper;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionEndpointSpec;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionProvider;
import org.apache.flink.statefun.flink.core.httpfn.TargetFunctions;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClientFactory;
import org.apache.flink.statefun.flink.core.spi.ExtensionResolver;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

/**
 * Version 2 {@link ComponentBinder} for binding a {@link HttpFunctionProvider}. Corresponding
 * {@link TypeName} is {@code io.statefun.endpoints.v2/http}.
 *
 * <p>Below is an example YAML document of the {@link ComponentJsonObject} recognized by this
 * binder, with the expected types of each field:
 *
 * <pre>
 * kind: io.statefun.endpoints.v2/http                                (typename)
 * spec:                                                              (object)
 *   functions: com.foo.bar/*                                         (typename)
 *   urlPathTemplate: https://bar.foo.com:8080/{function.name}        (string)
 *   maxNumBatchRequests: 10000                                       (int, optional)
 *   transports:                                                      (object, optional)
 *     type: io.statefun.transports.v1/okhttp                            (typename, optional)
 *     ...                                                            (remaining fields treated directly as properties)
 * </pre>
 */
final class HttpEndpointBinderV2 implements ComponentBinder {

  private static final ObjectMapper SPEC_OBJ_MAPPER = StateFunObjectMapper.create();

  static final HttpEndpointBinderV2 INSTANCE = new HttpEndpointBinderV2();

  static final TypeName KIND_TYPE = TypeName.parseFrom("io.statefun.endpoints.v2/http");

  private HttpEndpointBinderV2() {}

  @Override
  public void bind(ComponentJsonObject component, StatefulFunctionModule.Binder binder) {
    validateComponent(component);

    final HttpFunctionEndpointSpec spec = parseSpec(component);
    final HttpFunctionProvider provider = functionProvider(spec, getExtensionResolver(binder));

    final TargetFunctions target = spec.targetFunctions();
    if (target.isSpecificFunctionType()) {
      binder.bindFunctionProvider(target.asSpecificFunctionType(), provider);
    } else {
      binder.bindFunctionProvider(target.asNamespace(), provider);
    }
  }

  private static void validateComponent(ComponentJsonObject componentJsonObject) {
    final TypeName targetBinderType = componentJsonObject.binderTypename();
    if (!targetBinderType.equals(KIND_TYPE)) {
      throw new IllegalStateException(
          "Received unexpected ModuleComponent to bind: " + componentJsonObject);
    }
  }

  private static HttpFunctionEndpointSpec parseSpec(ComponentJsonObject component) {
    try {
      return SPEC_OBJ_MAPPER.treeToValue(component.specJsonNode(), HttpFunctionEndpointSpec.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error parsing a HttpFunctionEndpointSpec.", e);
    }
  }

  private static HttpFunctionProvider functionProvider(
      HttpFunctionEndpointSpec spec, ExtensionResolver extensionResolver) {
    final RequestReplyClientFactory transportClientFactory =
        extensionResolver.resolveExtension(
            spec.transportClientFactoryType(), RequestReplyClientFactory.class);
    return new HttpFunctionProvider(spec, transportClientFactory);
  }
}
