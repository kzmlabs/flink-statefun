// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.nettyclient;

import java.io.Closeable;
import java.util.function.BiConsumer;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.ReadOnlyHttpHeaders;

interface NettyClientService {

  void acquireChannel(BiConsumer<Channel, Throwable> consumer);

  void releaseChannel(Channel channel);

  String queryPath();

  ReadOnlyHttpHeaders headers();

  long totalRequestBudgetInNanos();

  Closeable newTimeout(Runnable client, long delayInNanos);

  void runOnEventLoop(Runnable task);

  boolean isShutdown();

  long systemNanoTime();

  <T> void writeAndFlush(T what, Channel ch, BiConsumer<Void, Throwable> listener);
}
