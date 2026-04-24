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

package org.apache.flink.statefun.e2e.k8s.util;

import java.net.URI;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.s3.S3Client;

/** AWS SDK v2 clients wired to a port-forwarded LocalStack endpoint with dummy credentials. */
public final class LocalStackClients {

  private static final StaticCredentialsProvider DUMMY_CREDS =
      StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));

  private LocalStackClients() {}

  public static KinesisClient kinesis(int localPort) {
    return KinesisClient.builder()
        .endpointOverride(endpoint(localPort))
        .region(Region.US_EAST_1)
        .credentialsProvider(DUMMY_CREDS)
        .build();
  }

  public static S3Client s3(int localPort) {
    return S3Client.builder()
        .endpointOverride(endpoint(localPort))
        .region(Region.US_EAST_1)
        .credentialsProvider(DUMMY_CREDS)
        .forcePathStyle(true) // LocalStack requires path-style
        .build();
  }

  private static URI endpoint(int localPort) {
    return URI.create("http://127.0.0.1:" + localPort);
  }
}
