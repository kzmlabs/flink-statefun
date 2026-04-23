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

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;
import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressSpec;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressStartupPosition;

public class KinesisSourceProvider implements SourceProvider {

  /** Formatter matching the connector's default {@code STREAM_TIMESTAMP_DATE_FORMAT}. */
  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

  @Override
  public <T, SplitT extends SourceSplit, EnumChckT> Source<T, SplitT, EnumChckT> forSpec(
      IngressSpec<T> ingressSpec) {
    KinesisIngressSpec<T> spec = asKinesisSpec(ingressSpec);

    Configuration sourceConfig = new Configuration();
    applyStartupPosition(sourceConfig, spec.startupPosition());

    Properties awsProps = new Properties();
    AwsConfigAppender.appendCredentials(awsProps, spec.awsCredentials().get());
    AwsConfigAppender.appendRegion(awsProps, spec.awsRegion().get());
    mergeProperties(sourceConfig, awsProps);
    mergeProperties(sourceConfig, spec.properties());

    KinesisStreamsSource<T> source =
        KinesisStreamsSource.<T>builder()
            .setStreamArn(resolveStreamArn(spec))
            .setSourceConfig(sourceConfig)
            .setDeserializationSchema(
                new KinesisDeserializationSchemaDelegate<>(spec.deserializer()))
            .build();

    //noinspection unchecked
    return (Source<T, SplitT, EnumChckT>) source;
  }

  private static <T> KinesisIngressSpec<T> asKinesisSpec(IngressSpec<T> ingressSpec) {
    if (ingressSpec instanceof KinesisIngressSpec) {
      return (KinesisIngressSpec<T>) ingressSpec;
    }
    if (ingressSpec == null) {
      throw new NullPointerException("Unable to translate a NULL spec");
    }
    throw new IllegalArgumentException(String.format("Wrong type %s", ingressSpec.type()));
  }

  private static String resolveStreamArn(KinesisIngressSpec<?> spec) {
    if (spec.streamArn() != null) {
      // ARN syntax validation is delegated to KinesisStreamsSource; we only validate presence here.
      return spec.streamArn();
    }
    if (spec.streams().size() == 1) {
      throw new IllegalArgumentException(
          "Flink 2.x KinesisStreamsSource requires an ARN. "
              + "Use KinesisIngressBuilder.withStreamArn(arn) instead of withStream(name).");
    }
    throw new IllegalArgumentException(
        "Multi-stream Kinesis ingress is not supported by the Flink 2.x runtime. "
            + "Use one KinesisIngressSpec per stream, configured via withStreamArn(arn).");
  }

  static void applyStartupPosition(Configuration cfg, KinesisIngressStartupPosition position) {
    if (position.isLatest()) {
      cfg.set(
          KinesisSourceConfigOptions.STREAM_INITIAL_POSITION,
          KinesisSourceConfigOptions.InitialPosition.LATEST);
      return;
    }
    if (position.isEarliest()) {
      cfg.set(
          KinesisSourceConfigOptions.STREAM_INITIAL_POSITION,
          KinesisSourceConfigOptions.InitialPosition.TRIM_HORIZON);
      return;
    }
    if (position.isDate()) {
      cfg.set(
          KinesisSourceConfigOptions.STREAM_INITIAL_POSITION,
          KinesisSourceConfigOptions.InitialPosition.AT_TIMESTAMP);
      ZonedDateTime zdt = position.asDate().date();
      cfg.set(KinesisSourceConfigOptions.STREAM_INITIAL_TIMESTAMP, TIMESTAMP_FORMATTER.format(zdt));
      return;
    }
    throw new IllegalStateException(
        "Safe guard; unreachable startup position variant: " + position);
  }

  private static void mergeProperties(Configuration cfg, Properties props) {
    props.forEach((k, v) -> cfg.setString(k.toString(), v.toString()));
  }
}
