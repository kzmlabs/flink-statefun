// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.spi;

import java.util.Map;
import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.IngressType;

public interface FlinkIoModule {

  void configure(Map<String, String> globalConfiguration, Binder binder);

  interface Binder {

    void bindSourceProvider(IngressType type, SourceProvider provider);

    void bindSinkProvider(EgressType type, SinkProvider provider);
  }
}
