// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.functions;

import java.util.Objects;
import java.util.function.Consumer;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.di.Lazy;
import org.apache.flink.statefun.flink.core.message.Message;

/**
 * Handles any of the delayed message that needs to be fired at a specific timestamp. This handler
 * dispatches {@linkplain Message}s to either remotely (shuffle) or locally.
 */
final class DelayMessageHandler implements Consumer<Message> {
  private final RemoteSink remoteSink;
  private final Lazy<Reductions> reductions;
  private final Partition thisPartition;

  @Inject
  public DelayMessageHandler(
      RemoteSink remoteSink,
      @Label("reductions") Lazy<Reductions> reductions,
      Partition partition) {
    this.remoteSink = Objects.requireNonNull(remoteSink);
    this.reductions = Objects.requireNonNull(reductions);
    this.thisPartition = Objects.requireNonNull(partition);
  }

  @Override
  public void accept(Message message) {
    if (thisPartition.contains(message.target())) {
      reductions.get().enqueue(message);
    } else {
      remoteSink.accept(message);
    }
  }

  public void onStart() {}

  public void onComplete() {
    reductions.get().processEnvelopes();
  }
}
