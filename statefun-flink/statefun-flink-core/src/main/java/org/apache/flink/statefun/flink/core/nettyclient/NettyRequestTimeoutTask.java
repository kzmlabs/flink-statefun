// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.nettyclient;

import static org.apache.flink.util.Preconditions.checkState;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.ScheduledFuture;
import org.apache.flink.statefun.flink.core.nettyclient.exceptions.RequestTimeoutException;

final class NettyRequestTimeoutTask implements Runnable {
  private final NettyRequestReplyHandler handler;
  @Nullable private ScheduledFuture<?> future;
  @Nullable private ChannelHandlerContext ctx;

  public NettyRequestTimeoutTask(NettyRequestReplyHandler handler) {
    this.handler = Objects.requireNonNull(handler);
  }

  void schedule(ChannelHandlerContext ctx, long remainingRequestBudget) {
    this.ctx = Objects.requireNonNull(ctx);
    this.future = ctx.executor().schedule(this, remainingRequestBudget, TimeUnit.NANOSECONDS);
  }

  void cancel() {
    if (future != null) {
      future.cancel(false);
      future = null;
    }
    ctx = null;
  }

  @Override
  public void run() {
    checkState(ctx != null);
    checkState(future != null);
    handler.exceptionCaught(ctx, RequestTimeoutException.INSTANCE);
  }
}
