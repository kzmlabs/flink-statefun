// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.io.kinesis;

import java.util.Properties;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;

final class AwsConfigAppender {

  private AwsConfigAppender() {}

  static void appendCredentials(Properties props, AwsCredentials credentials) {
    if (credentials.isDefault()) {
      props.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");
      return;
    }
    if (credentials.isBasic()) {
      props.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
      props.setProperty(
          AWSConfigConstants.accessKeyId(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
          credentials.asBasic().accessKeyId());
      props.setProperty(
          AWSConfigConstants.secretKey(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
          credentials.asBasic().secretAccessKey());
      return;
    }
    if (credentials.isProfile()) {
      props.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "PROFILE");
      props.setProperty(
          AWSConfigConstants.profileName(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
          credentials.asProfile().name());
      credentials
          .asProfile()
          .path()
          .ifPresent(
              p ->
                  props.setProperty(
                      AWSConfigConstants.profilePath(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
                      p));
      return;
    }
    throw new IllegalStateException(
        "Safe guard; unreachable AwsCredentials variant: " + credentials);
  }

  static void appendRegion(Properties props, AwsRegion region) {
    if (region.isDefault()) {
      return;
    }
    if (region.isId()) {
      props.setProperty(AWSConfigConstants.AWS_REGION, region.asId().id());
      return;
    }
    if (region.isCustomEndpoint()) {
      AwsRegion.CustomEndpointAwsRegion r = region.asCustomEndpoint();
      props.setProperty(AWSConfigConstants.AWS_REGION, r.regionId());
      props.setProperty(AWSConfigConstants.AWS_ENDPOINT, r.serviceEndpoint());
      return;
    }
    throw new IllegalStateException("Safe guard; unreachable AwsRegion variant: " + region);
  }
}
