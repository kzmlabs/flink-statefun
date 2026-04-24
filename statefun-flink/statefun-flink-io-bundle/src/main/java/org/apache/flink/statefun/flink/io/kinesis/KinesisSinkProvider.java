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
package org.apache.flink.statefun.flink.io.kinesis;

import java.util.Properties;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.statefun.flink.io.common.ReflectionUtil;
import org.apache.flink.statefun.flink.io.spi.SinkProvider;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSerializer;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSpec;

public class KinesisSinkProvider implements SinkProvider {

  @Override
  public <T> Sink<T> forSpec(EgressSpec<T> egressSpec) {
    KinesisEgressSpec<T> spec = asSpec(egressSpec);

    Properties awsProps = new Properties();
    AwsConfigAppender.appendCredentials(awsProps, spec.awsCredentials());
    AwsConfigAppender.appendRegion(awsProps, spec.awsRegion());
    awsProps.putAll(spec.clientConfigurationProperties());

    KinesisEgressSerializer<T> serializer = ReflectionUtil.instantiate(spec.serializerClass());
    KinesisSerializationSchemaDelegate<T> delegate =
        new KinesisSerializationSchemaDelegate<>(serializer);

    return KinesisStreamsSink.<T>builder()
        .setStreamName(spec.streamName())
        .setKinesisClientProperties(awsProps)
        .setSerializationSchema(delegate)
        .setPartitionKeyGenerator(delegate.partitionKeyGenerator())
        .setMaxBufferedRequests(spec.maxOutstandingRecords())
        .build();
  }

  private static <T> KinesisEgressSpec<T> asSpec(EgressSpec<T> spec) {
    if (spec == null) {
      throw new NullPointerException("Unable to translate a NULL spec");
    }
    if (spec instanceof KinesisEgressSpec) {
      return (KinesisEgressSpec<T>) spec;
    }
    throw new IllegalArgumentException(String.format("Wrong type %s", spec.type()));
  }
}
