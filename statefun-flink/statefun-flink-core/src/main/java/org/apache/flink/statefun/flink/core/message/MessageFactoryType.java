// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.message;

public enum MessageFactoryType {
  WITH_KRYO_PAYLOADS,
  WITH_PROTOBUF_PAYLOADS,
  WITH_RAW_PAYLOADS,
  WITH_CUSTOM_PAYLOADS,
}
