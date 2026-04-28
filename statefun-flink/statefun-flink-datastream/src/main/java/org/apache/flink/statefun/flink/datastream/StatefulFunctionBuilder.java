// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.datastream;

import java.net.URI;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.flink.common.json.StateFunObjectMapper;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionEndpointSpec;
import org.apache.flink.statefun.sdk.FunctionType;

/** Base class for statefun function builders. */
public abstract class StatefulFunctionBuilder {

  /** The object mapper used to serialize the client spec object. */
  static final ObjectMapper CLIENT_SPEC_OBJ_MAPPER = StateFunObjectMapper.create();

  /**
   * Override to provide the endpoint spec.
   *
   * @return The endpoint spec.
   */
  abstract HttpFunctionEndpointSpec spec();

  /**
   * Creates a function builder using the synchronous HTTP protocol.
   *
   * @param functionType the function type that is served remotely.
   * @param endpoint the endpoint that serves that remote function.
   * @return a builder.
   */
  public static RequestReplyFunctionBuilder requestReplyFunctionBuilder(
      FunctionType functionType, URI endpoint) {
    return new RequestReplyFunctionBuilder(functionType, endpoint);
  }

  /**
   * Creates a function builder using the asynchronous HTTP protocol.
   *
   * @param functionType the function type that is served remotely.
   * @param endpoint the endpoint that serves that remote function.
   * @return a builder.
   */
  public static AsyncRequestReplyFunctionBuilder asyncRequestReplyFunctionBuilder(
      FunctionType functionType, URI endpoint) {
    return new AsyncRequestReplyFunctionBuilder(functionType, endpoint);
  }
}
