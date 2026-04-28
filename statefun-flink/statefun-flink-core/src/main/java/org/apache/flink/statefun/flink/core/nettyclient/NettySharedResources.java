// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.nettyclient;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.Epoll;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.kqueue.KQueue;
import org.apache.flink.shaded.netty4.io.netty.channel.kqueue.KQueueEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.kqueue.KQueueSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.flink.util.IOUtils;

final class NettySharedResources {
  private final AtomicBoolean shutdown = new AtomicBoolean();
  private final Bootstrap bootstrap;

  private final CloseableRegistry mangedResources = new CloseableRegistry();

  public NettySharedResources() {
    // TODO: configure DNS resolving
    final EventLoopGroup workerGroup;
    final Class<? extends Channel> channelClass;
    if (Epoll.isAvailable()) {
      workerGroup = new EpollEventLoopGroup(demonThreadFactory("netty-http-worker"));
      channelClass = EpollSocketChannel.class;
    } else if (KQueue.isAvailable()) {
      workerGroup = new KQueueEventLoopGroup(demonThreadFactory("http-netty-worker"));
      channelClass = KQueueSocketChannel.class;
    } else {
      workerGroup = new NioEventLoopGroup(demonThreadFactory("netty-http-client"));
      channelClass = NioSocketChannel.class;
    }
    registerClosable(workerGroup::shutdownGracefully);

    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(workerGroup);
    bootstrap.channel(channelClass);

    this.bootstrap = bootstrap;
  }

  public Bootstrap bootstrap() {
    return bootstrap;
  }

  public void registerClosable(Closeable closeable) {
    try {
      mangedResources.registerCloseable(closeable);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public boolean isShutdown() {
    return shutdown.get();
  }

  public void shutdownGracefully() {
    if (shutdown.compareAndSet(false, true)) {
      IOUtils.closeQuietly(mangedResources);
    }
  }

  private static ThreadFactory demonThreadFactory(String name) {
    return runnable -> {
      Thread t = new Thread(runnable);
      t.setDaemon(true);
      t.setName(name);
      return t;
    };
  }
}
