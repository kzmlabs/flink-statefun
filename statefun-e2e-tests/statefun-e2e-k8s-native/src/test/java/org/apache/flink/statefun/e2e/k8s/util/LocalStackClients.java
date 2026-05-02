// SPDX-License-Identifier: Apache-2.0

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
