// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.httpfn;

import java.net.URI;
import java.util.Objects;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.flink.statefun.flink.core.common.ManagingResources;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClientFactory;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyFunction;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;

@NotThreadSafe
public final class HttpFunctionProvider implements StatefulFunctionProvider, ManagingResources {

  private final HttpFunctionEndpointSpec endpointSpec;
  private final RequestReplyClientFactory requestReplyClientFactory;

  public HttpFunctionProvider(
      HttpFunctionEndpointSpec endpointSpec, RequestReplyClientFactory requestReplyClientFactory) {
    this.endpointSpec = Objects.requireNonNull(endpointSpec);
    this.requestReplyClientFactory = Objects.requireNonNull(requestReplyClientFactory);
  }

  @Override
  public StatefulFunction functionOfType(FunctionType functionType) {
    final URI endpointUrl = endpointSpec.urlPathTemplate().apply(functionType);

    return new RequestReplyFunction(
        functionType,
        endpointSpec.maxNumBatchRequests(),
        requestReplyClientFactory.createTransportClient(
            endpointSpec.transportClientProperties(), endpointUrl));
  }

  @Override
  public void shutdown() {
    requestReplyClientFactory.cleanup();
  }
}
