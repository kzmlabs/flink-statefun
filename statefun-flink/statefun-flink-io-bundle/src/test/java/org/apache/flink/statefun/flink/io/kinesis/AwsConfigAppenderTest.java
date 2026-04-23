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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;
import org.junit.jupiter.api.Test;

class AwsConfigAppenderTest {

  @Test
  void appendsBasicCredentials() {
    Properties props = new Properties();
    AwsConfigAppender.appendCredentials(props, AwsCredentials.basic("AKIAFAKE", "secret/FAKE"));

    assertThat(props).containsEntry(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
    assertThat(props)
        .containsEntry(
            AWSConfigConstants.accessKeyId(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
            "AKIAFAKE");
    assertThat(props)
        .containsEntry(
            AWSConfigConstants.secretKey(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
            "secret/FAKE");
  }

  @Test
  void appendsDefaultProviderChain() {
    Properties props = new Properties();
    AwsConfigAppender.appendCredentials(props, AwsCredentials.fromDefaultProviderChain());
    assertThat(props).containsEntry(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");
  }

  @Test
  void appendsProfileWithoutPath() {
    Properties props = new Properties();
    AwsConfigAppender.appendCredentials(props, AwsCredentials.profile("myprofile"));
    assertThat(props).containsEntry(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "PROFILE");
    assertThat(props)
        .containsEntry(
            AWSConfigConstants.profileName(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
            "myprofile");
    // path absent
    assertThat(props)
        .doesNotContainKey(
            AWSConfigConstants.profilePath(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER));
  }

  @Test
  void appendsProfileWithPath() {
    Properties props = new Properties();
    AwsConfigAppender.appendCredentials(
        props, AwsCredentials.profile("myprofile", "/path/to/profile"));
    assertThat(props).containsEntry(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "PROFILE");
    assertThat(props)
        .containsEntry(
            AWSConfigConstants.profileName(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
            "myprofile");
    assertThat(props)
        .containsEntry(
            AWSConfigConstants.profilePath(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
            "/path/to/profile");
  }

  @Test
  void appendsDefaultRegion() {
    Properties props = new Properties();
    AwsConfigAppender.appendRegion(props, AwsRegion.fromDefaultProviderChain());
    assertThat(props).isEmpty();
  }

  @Test
  void appendsSpecificRegion() {
    Properties props = new Properties();
    AwsConfigAppender.appendRegion(props, AwsRegion.ofId("us-east-1"));
    assertThat(props).containsEntry(AWSConfigConstants.AWS_REGION, "us-east-1");
    assertThat(props).doesNotContainKey(AWSConfigConstants.AWS_ENDPOINT);
  }

  @Test
  void appendsCustomEndpoint() {
    Properties props = new Properties();
    AwsConfigAppender.appendRegion(
        props, AwsRegion.ofCustomEndpoint("https://localstack:4566", "us-east-1"));
    assertThat(props).containsEntry(AWSConfigConstants.AWS_REGION, "us-east-1");
    assertThat(props).containsEntry(AWSConfigConstants.AWS_ENDPOINT, "https://localstack:4566");
  }
}
