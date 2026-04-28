// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.reqreply;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;

@PublicEvolving
public interface RequestReplyClient {

  CompletableFuture<FromFunction> call(
      ToFunctionRequestSummary requestSummary,
      RemoteInvocationMetrics metrics,
      ToFunction toFunction);
}
