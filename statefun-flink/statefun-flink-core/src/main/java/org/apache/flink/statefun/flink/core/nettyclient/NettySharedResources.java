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
import org.apache.flink.shaded.netty4.io.netty.channel.IoHandlerFactory;
import org.apache.flink.shaded.netty4.io.netty.channel.MultiThreadIoEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.Epoll;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollIoHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.kqueue.KQueue;
import org.apache.flink.shaded.netty4.io.netty.channel.kqueue.KQueueIoHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.kqueue.KQueueSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioIoHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.flink.util.IOUtils;

final class NettySharedResources {
  private final AtomicBoolean shutdown = new AtomicBoolean();
  private final Bootstrap bootstrap;

  private final CloseableRegistry mangedResources = new CloseableRegistry();

  public NettySharedResources() {
    // TODO: configure DNS resolving
    // Netty 4.2: transport-specific EventLoopGroups are deprecated in favour of
    // MultiThreadIoEventLoopGroup driven by a transport IoHandlerFactory.
    final IoHandlerFactory ioHandlerFactory;
    final Class<? extends Channel> channelClass;
    final String threadName;
    if (Epoll.isAvailable()) {
      ioHandlerFactory = EpollIoHandler.newFactory();
      channelClass = EpollSocketChannel.class;
      threadName = "netty-http-worker";
    } else if (KQueue.isAvailable()) {
      ioHandlerFactory = KQueueIoHandler.newFactory();
      channelClass = KQueueSocketChannel.class;
      threadName = "http-netty-worker";
    } else {
      ioHandlerFactory = NioIoHandler.newFactory();
      channelClass = NioSocketChannel.class;
      threadName = "netty-http-client";
    }
    final EventLoopGroup workerGroup =
        new MultiThreadIoEventLoopGroup(demonThreadFactory(threadName), ioHandlerFactory);
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
