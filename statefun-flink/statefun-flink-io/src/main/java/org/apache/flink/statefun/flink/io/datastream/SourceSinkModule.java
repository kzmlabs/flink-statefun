// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.datastream;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.statefun.flink.io.spi.FlinkIoModule;
import org.apache.flink.statefun.flink.io.spi.SinkProvider;
import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.io.IngressSpec;

@AutoService(FlinkIoModule.class)
public class SourceSinkModule implements FlinkIoModule {

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    SinkSourceProvider provider = new SinkSourceProvider();

    binder.bindSourceProvider(SourceFunctionSpec.TYPE, provider);
    binder.bindSinkProvider(SinkFunctionSpec.TYPE, provider);
  }

  private static final class SinkSourceProvider implements SourceProvider, SinkProvider {

    @Override
    public <T, SplitT extends SourceSplit, EnumChckT> Source<T, SplitT, EnumChckT> forSpec(
        IngressSpec<T> spec) {
      if (!(spec instanceof SourceFunctionSpec)) {
        throw new IllegalStateException("spec " + spec + " is not of type SourceFunctionSpec");
      }
      SourceFunctionSpec<T, SplitT, EnumChckT> casted =
          (SourceFunctionSpec<T, SplitT, EnumChckT>) spec;
      return casted.delegate();
    }

    @Override
    public <T> Sink<T> forSpec(EgressSpec<T> spec) {
      if (!(spec instanceof SinkFunctionSpec)) {
        throw new IllegalStateException("spec " + spec + " is not of type SourceFunctionSpec");
      }
      SinkFunctionSpec<T> casted = (SinkFunctionSpec<T>) spec;
      return casted.delegate();
    }
  }
}
