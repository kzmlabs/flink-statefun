// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.spi;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.statefun.sdk.io.EgressSpec;

public interface SinkProvider {

  <T> Sink<T> forSpec(EgressSpec<T> spec);
}
