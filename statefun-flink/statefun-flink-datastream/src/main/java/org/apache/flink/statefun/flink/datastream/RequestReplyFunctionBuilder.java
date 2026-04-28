// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.datastream;

import java.net.URI;
import java.time.Duration;
import org.apache.flink.annotation.Internal;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.core.httpfn.*;
import org.apache.flink.statefun.sdk.FunctionType;

/** A builder for RequestReply remote function type. */
public class RequestReplyFunctionBuilder extends StatefulFunctionBuilder {

  /**
   * Create a new builder for a remote function with a given type and an endpoint.
   *
   * @deprecated Use {@link StatefulFunctionBuilder#requestReplyFunctionBuilder} instead.
   * @param functionType the function type that is served remotely.
   * @param endpoint the endpoint that serves that remote function.
   * @return a builder.
   */
  @Deprecated
  public static RequestReplyFunctionBuilder requestReplyFunctionBuilder(
      FunctionType functionType, URI endpoint) {
    return new RequestReplyFunctionBuilder(functionType, endpoint);
  }

  private final DefaultHttpRequestReplyClientSpec.Timeouts transportClientTimeoutsSpec =
      new DefaultHttpRequestReplyClientSpec.Timeouts();

  private final HttpFunctionEndpointSpec.Builder builder;

  RequestReplyFunctionBuilder(FunctionType functionType, URI endpoint) {
    this.builder =
        HttpFunctionEndpointSpec.builder(
            TargetFunctions.functionType(functionType),
            new UrlPathTemplate(endpoint.toASCIIString()));
  }

  /**
   * Set a maximum request duration. This duration spans the complete call, including connecting to
   * the function endpoint, writing the request, function processing, and reading the response.
   *
   * @param duration the duration after which the request is considered failed.
   * @return this builder.
   */
  public RequestReplyFunctionBuilder withMaxRequestDuration(Duration duration) {
    transportClientTimeoutsSpec.setCallTimeout(duration);
    return this;
  }

  /**
   * Set a timeout for connecting to function endpoints.
   *
   * @param duration the duration after which a connect attempt is considered failed.
   * @return this builder.
   */
  public RequestReplyFunctionBuilder withConnectTimeout(Duration duration) {
    transportClientTimeoutsSpec.setConnectTimeout(duration);
    return this;
  }

  /**
   * Set a timeout for individual read IO operations during a function invocation request.
   *
   * @param duration the duration after which a read IO operation is considered failed.
   * @return this builder.
   */
  public RequestReplyFunctionBuilder withReadTimeout(Duration duration) {
    transportClientTimeoutsSpec.setReadTimeout(duration);
    return this;
  }

  /**
   * Set a timeout for individual write IO operations during a function invocation request.
   *
   * @param duration the duration after which a write IO operation is considered failed.
   * @return this builder.
   */
  public RequestReplyFunctionBuilder withWriteTimeout(Duration duration) {
    transportClientTimeoutsSpec.setWriteTimeout(duration);
    return this;
  }

  /**
   * Sets the max messages to batch together for a specific address.
   *
   * @param maxNumBatchRequests the maximum number of requests to batch for an address.
   * @return this builder.
   */
  public RequestReplyFunctionBuilder withMaxNumBatchRequests(int maxNumBatchRequests) {
    builder.withMaxNumBatchRequests(maxNumBatchRequests);
    return this;
  }

  /**
   * Create the endpoint spec for the function.
   *
   * @return The endpoint spec.
   */
  @Internal
  @Override
  HttpFunctionEndpointSpec spec() {
    final TransportClientSpec transportClientSpec =
        new TransportClientSpec(
            TransportClientConstants.OKHTTP_CLIENT_FACTORY_TYPE,
            transportClientPropertiesAsObjectNode(transportClientTimeoutsSpec));
    builder.withTransport(transportClientSpec);
    return builder.build();
  }

  private static ObjectNode transportClientPropertiesAsObjectNode(
      DefaultHttpRequestReplyClientSpec.Timeouts transportClientTimeoutsSpec) {
    final DefaultHttpRequestReplyClientSpec transportClientSpecPojo =
        new DefaultHttpRequestReplyClientSpec();
    transportClientSpecPojo.setTimeouts(transportClientTimeoutsSpec);

    return transportClientSpecPojo.toJson(CLIENT_SPEC_OBJ_MAPPER);
  }
}
