/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
