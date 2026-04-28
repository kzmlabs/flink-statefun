// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.datastream;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.statefun.flink.core.httpfn.DefaultHttpRequestReplyClientFactory;
import org.apache.flink.statefun.flink.core.httpfn.TransportClientConstants;
import org.apache.flink.statefun.flink.core.nettyclient.NettyRequestReplyClientFactory;
import org.junit.jupiter.api.Test;

public class SerializableHttpFunctionProviderTest {

  /** Validate the mapping from transport type to client-factory type. */
  @Test
  public void functionProviderShouldUseProperClientFactory() {
    assertEquals(
        DefaultHttpRequestReplyClientFactory.INSTANCE,
        SerializableHttpFunctionProvider.getClientFactory(
            TransportClientConstants.OKHTTP_CLIENT_FACTORY_TYPE));
    assertEquals(
        NettyRequestReplyClientFactory.INSTANCE,
        SerializableHttpFunctionProvider.getClientFactory(
            TransportClientConstants.ASYNC_CLIENT_FACTORY_TYPE));
  }
}
