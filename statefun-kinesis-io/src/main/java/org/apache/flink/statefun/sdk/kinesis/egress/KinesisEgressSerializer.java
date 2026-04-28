// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.kinesis.egress;

import java.io.Serializable;

/**
 * Defines how to serialize values of type {@code T} into {@link EgressRecord}s to be written to AWS
 * Kinesis.
 *
 * @param <T> the type of values being written.
 */
public interface KinesisEgressSerializer<T> extends Serializable {

  /**
   * Serialize an output value into a {@link EgressRecord} to be written to AWS Kinesis.
   *
   * @param value the output value to write.
   * @return a {@link EgressRecord} to be written to AWS Kinesis.
   */
  EgressRecord serialize(T value);
}
