// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.kinesis.ingress;

import java.io.Serializable;

/**
 * Describes how to deserialize {@link IngressRecord}s consumed from AWS Kinesis into data types
 * that are processed by the system.
 *
 * @param <T> The type created by the ingress deserializer.
 */
public interface KinesisIngressDeserializer<T> extends Serializable {

  /**
   * Deserialize an input value from a {@link IngressRecord} consumed from AWS Kinesis.
   *
   * @param ingressRecord the {@link IngressRecord} consumed from AWS Kinesis.
   * @return the deserialized data object.
   */
  T deserialize(IngressRecord ingressRecord);
}
