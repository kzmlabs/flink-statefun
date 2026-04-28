// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.httpfn;

import static org.apache.flink.statefun.flink.core.httpfn.OkHttpUnixSocketBridge.configureUnixDomainSocket;

import java.net.URI;
import javax.annotation.Nullable;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.common.SetContextClassLoader;
import org.apache.flink.statefun.flink.common.json.StateFunObjectMapper;
import org.apache.flink.statefun.flink.core.reqreply.ClassLoaderSafeRequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClientFactory;

public final class DefaultHttpRequestReplyClientFactory implements RequestReplyClientFactory {

  public static final DefaultHttpRequestReplyClientFactory INSTANCE =
      new DefaultHttpRequestReplyClientFactory();

  private static final ObjectMapper OBJ_MAPPER = StateFunObjectMapper.create();

  /** lazily initialized by {@link #createTransportClient} */
  @Nullable private volatile OkHttpClient sharedClient;

  private DefaultHttpRequestReplyClientFactory() {}

  @Override
  public RequestReplyClient createTransportClient(ObjectNode transportProperties, URI endpointUrl) {
    final DefaultHttpRequestReplyClient client = createClient(transportProperties, endpointUrl);

    if (Thread.currentThread().getContextClassLoader() == getClass().getClassLoader()) {
      return client;
    } else {
      return new ClassLoaderSafeRequestReplyClient(client);
    }
  }

  @Override
  public void cleanup() {
    final OkHttpClient sharedClient = this.sharedClient;
    this.sharedClient = null;
    OkHttpUtils.closeSilently(sharedClient);
  }

  private DefaultHttpRequestReplyClient createClient(
      ObjectNode transportProperties, URI endpointUrl) {
    try (SetContextClassLoader ignored = new SetContextClassLoader(this)) {
      OkHttpClient sharedClient = this.sharedClient;
      if (sharedClient == null) {
        sharedClient = OkHttpUtils.newClient();
        this.sharedClient = sharedClient;
      }
      final OkHttpClient.Builder clientBuilder = sharedClient.newBuilder();

      final DefaultHttpRequestReplyClientSpec transportClientSpec =
          parseTransportProperties(transportProperties);

      clientBuilder.callTimeout(transportClientSpec.getTimeouts().getCallTimeout());
      clientBuilder.connectTimeout(transportClientSpec.getTimeouts().getConnectTimeout());
      clientBuilder.readTimeout(transportClientSpec.getTimeouts().getReadTimeout());
      clientBuilder.writeTimeout(transportClientSpec.getTimeouts().getWriteTimeout());

      HttpUrl url;
      if (UnixDomainHttpEndpoint.validate(endpointUrl)) {
        UnixDomainHttpEndpoint endpoint = UnixDomainHttpEndpoint.parseFrom(endpointUrl);

        url =
            new HttpUrl.Builder()
                .scheme("http")
                .host("unused")
                .addPathSegment(endpoint.pathSegment)
                .build();

        configureUnixDomainSocket(clientBuilder, endpoint.unixDomainFile);
      } else {
        url = HttpUrl.get(endpointUrl);
      }

      return new DefaultHttpRequestReplyClient(
          url, clientBuilder.build(), () -> isShutdown(this.sharedClient));
    }
  }

  private boolean isShutdown(OkHttpClient previousClient) {
    return DefaultHttpRequestReplyClientFactory.this.sharedClient != previousClient;
  }

  private static DefaultHttpRequestReplyClientSpec parseTransportProperties(
      ObjectNode transportClientProperties) {
    try {
      return DefaultHttpRequestReplyClientSpec.fromJson(OBJ_MAPPER, transportClientProperties);
    } catch (Exception e) {
      throw new RuntimeException(
          "Unable to parse transport client properties when creating client: ", e);
    }
  }
}
