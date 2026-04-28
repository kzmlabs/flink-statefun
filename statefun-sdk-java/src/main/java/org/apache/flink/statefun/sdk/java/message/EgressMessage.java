// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.sdk.java.message;

import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.slice.Slice;

public interface EgressMessage {
  TypeName targetEgressId();

  TypeName egressMessageValueType();

  Slice egressMessageValueBytes();
}
