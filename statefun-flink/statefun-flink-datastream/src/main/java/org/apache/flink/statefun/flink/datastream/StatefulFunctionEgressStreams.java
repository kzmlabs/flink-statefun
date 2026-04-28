// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.datastream;

import java.util.Map;
import java.util.Objects;
import org.apache.flink.annotation.Internal;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * StatefulFunctionEgressStreams - this class holds a handle for every egress stream defined via
 * {@link StatefulFunctionDataStreamBuilder#withEgressId(EgressIdentifier)}. see {@link
 * #getDataStreamForEgressId(EgressIdentifier)}.
 */
public final class StatefulFunctionEgressStreams {
  private final Map<EgressIdentifier<?>, DataStream<?>> egresses;

  @Internal
  StatefulFunctionEgressStreams(Map<EgressIdentifier<?>, DataStream<?>> egresses) {
    this.egresses = Objects.requireNonNull(egresses);
  }

  /**
   * Returns the {@link DataStream} that represents a stateful functions egress for an {@link
   * EgressIdentifier}.
   *
   * <p>Messages that are sent to an egress with the supplied id, (via {@link
   * org.apache.flink.statefun.sdk.Context#send(EgressIdentifier, Object)}) would result in the
   * {@link DataStream} returned from that method.
   *
   * @param id the egress id, as provided to {@link
   *     StatefulFunctionDataStreamBuilder#withEgressId(EgressIdentifier)}.
   * @param <T> the egress message type.
   * @return a data stream that represents messages sent to the provided egress.
   */
  @SuppressWarnings("unchecked")
  public <T> DataStream<T> getDataStreamForEgressId(EgressIdentifier<T> id) {
    Objects.requireNonNull(id);
    DataStream<?> dataStream = egresses.get(id);
    if (dataStream == null) {
      throw new IllegalArgumentException("Unknown data stream for egress " + id);
    }
    return (DataStream<T>) dataStream;
  }
}
