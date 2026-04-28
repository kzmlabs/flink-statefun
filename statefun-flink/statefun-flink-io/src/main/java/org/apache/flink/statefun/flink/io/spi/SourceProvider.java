// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.spi;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.statefun.sdk.io.IngressSpec;

public interface SourceProvider {

  <T, SplitT extends SourceSplit, EnumChckT> Source<T, SplitT, EnumChckT> forSpec(
      IngressSpec<T> spec);
}
