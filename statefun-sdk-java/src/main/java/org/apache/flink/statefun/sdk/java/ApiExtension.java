// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.sdk.java;

import org.apache.flink.statefun.sdk.java.annotations.Internal;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.ByteString;

@Internal
public final class ApiExtension {

  public static ByteString typeNameByteString(TypeName typeName) {
    return typeName.typeNameByteString();
  }

  public static ByteString stateNameByteString(ValueSpec<?> spec) {
    return spec.nameByteString();
  }
}
