// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.metrics;

import com.codahale.metrics.UniformReservoir;
import java.util.Objects;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.statefun.sdk.metrics.Metrics;

final class FlinkFunctionTypeMetrics implements FunctionTypeMetrics {
  private final Counter incoming;
  private final Counter outgoingLocalMessage;
  private final Counter outgoingRemoteMessage;
  private final Counter outgoingEgress;
  private final Counter blockedAddress;
  private final Counter inflightAsyncOps;
  private final Counter backlogMessage;
  private final Counter remoteInvocationFailures;
  private final Histogram remoteInvocationLatency;
  private final Metrics functionTypeScopedMetrics;

  FlinkFunctionTypeMetrics(MetricGroup typeGroup, Metrics functionTypeScopedMetrics) {
    this.incoming = metered(typeGroup, "in");
    this.outgoingLocalMessage = metered(typeGroup, "outLocal");
    this.outgoingRemoteMessage = metered(typeGroup, "outRemote");
    this.outgoingEgress = metered(typeGroup, "outEgress");
    this.blockedAddress = typeGroup.counter("numBlockedAddress");
    this.inflightAsyncOps = typeGroup.counter("inflightAsyncOps");
    this.backlogMessage = typeGroup.counter("numBacklog", new NonNegativeCounter());
    this.remoteInvocationFailures = metered(typeGroup, "remoteInvocationFailures");
    this.remoteInvocationLatency = typeGroup.histogram("remoteInvocationLatency", histogram());
    this.functionTypeScopedMetrics = Objects.requireNonNull(functionTypeScopedMetrics);
  }

  @Override
  public void incomingMessage() {
    incoming.inc();
  }

  @Override
  public void outgoingLocalMessage() {
    this.outgoingLocalMessage.inc();
  }

  @Override
  public void outgoingRemoteMessage() {
    this.outgoingRemoteMessage.inc();
  }

  @Override
  public void outgoingEgressMessage() {
    this.outgoingEgress.inc();
  }

  @Override
  public void blockedAddress() {
    this.blockedAddress.inc();
  }

  @Override
  public void unblockedAddress() {
    this.blockedAddress.dec();
  }

  @Override
  public void asyncOperationRegistered() {
    this.inflightAsyncOps.inc();
  }

  @Override
  public void asyncOperationCompleted() {
    this.inflightAsyncOps.dec();
  }

  @Override
  public void appendBacklogMessages(int count) {
    backlogMessage.inc(count);
  }

  @Override
  public void consumeBacklogMessages(int count) {
    backlogMessage.dec(count);
  }

  @Override
  public Metrics functionTypeScopedMetrics() {
    return functionTypeScopedMetrics;
  }

  @Override
  public void remoteInvocationFailures() {
    remoteInvocationFailures.inc();
  }

  @Override
  public void remoteInvocationLatency(long elapsed) {
    remoteInvocationLatency.update(elapsed);
  }

  private static SimpleCounter metered(MetricGroup metrics, String name) {
    SimpleCounter counter = metrics.counter(name, new SimpleCounter());
    metrics.meter(name + "Rate", new MeterView(counter, 60));
    return counter;
  }

  private static DropwizardHistogramWrapper histogram() {
    com.codahale.metrics.Histogram dropwizardHistogram =
        new com.codahale.metrics.Histogram(new UniformReservoir());
    return new DropwizardHistogramWrapper(dropwizardHistogram);
  }
}
