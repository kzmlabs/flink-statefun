// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.nettyclient;

import java.util.Objects;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelDuplexHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.channel.pool.ChannelPoolHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpClientCodec;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpContentDecompressor;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObjectAggregator;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;

final class HttpConnectionPoolManager implements ChannelPoolHandler {
  private final NettyRequestReplySpec spec;
  private final SslContext sslContext;
  private final String peerHost;
  private final int peerPort;
  private final Supplier<ChannelDuplexHandler> requestReplyHandlerSupplier;

  public HttpConnectionPoolManager(
      @Nullable SslContext sslContext,
      NettyRequestReplySpec spec,
      String peerHost,
      int peerPort,
      Supplier<ChannelDuplexHandler> requestReplyHandlerSupplier) {
    this.spec = Objects.requireNonNull(spec);
    this.peerHost = Objects.requireNonNull(peerHost);
    this.sslContext = sslContext;
    this.peerPort = peerPort;
    this.requestReplyHandlerSupplier = requestReplyHandlerSupplier;
  }

  @Override
  public void channelAcquired(Channel channel) {
    channel.attr(ChannelAttributes.ACQUIRED).set(Boolean.TRUE);
  }

  @Override
  public void channelReleased(Channel channel) {
    channel.attr(ChannelAttributes.ACQUIRED).set(Boolean.FALSE);
    NettyRequestReplyHandler handler = channel.pipeline().get(NettyRequestReplyHandler.class);
    handler.onReleaseToPool();
  }

  @Override
  public void channelCreated(Channel channel) {
    ChannelPipeline p = channel.pipeline();
    if (sslContext != null) {
      SslHandler sslHandler = sslContext.newHandler(channel.alloc(), peerHost, peerPort);
      p.addLast(sslHandler);
    }
    p.addLast(new HttpClientCodec());
    p.addLast(new HttpContentDecompressor(true));
    p.addLast(new HttpObjectAggregator(spec.maxRequestOrResponseSizeInBytes, true));
    p.addLast(requestReplyHandlerSupplier.get());

    long channelTimeToLiveMillis = spec.pooledConnectionTTL.toMillis();
    p.addLast(new HttpConnectionPoolHandler(channelTimeToLiveMillis));
  }
}
