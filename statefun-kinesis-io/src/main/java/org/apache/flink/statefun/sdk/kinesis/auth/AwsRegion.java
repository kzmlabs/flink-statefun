// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.kinesis.auth;

import java.net.URI;
import java.util.Objects;

/** AWS region to use for connecting to AWS Kinesis. */
public abstract class AwsRegion {

  private AwsRegion() {}

  /** Consults AWS's default provider chain to determine the AWS region. */
  public static AwsRegion fromDefaultProviderChain() {
    return DefaultAwsRegion.INSTANCE;
  }

  /** Specifies an AWS region using the region's unique id. */
  public static AwsRegion ofId(String id) {
    return new SpecificIdAwsRegion(id);
  }

  /**
   * Connects to an AWS region through a non-standard AWS service endpoint. This is typically used
   * only for development and testing purposes.
   */
  public static AwsRegion ofCustomEndpoint(String serviceEndpoint, String regionId) {
    return new CustomEndpointAwsRegion(serviceEndpoint, regionId);
  }

  /** Checks whether the region is configured to be obtained from AWS's default provider chain. */
  public boolean isDefault() {
    return getClass() == DefaultAwsRegion.class;
  }

  /** Checks whether the region is specified with the region's unique id. */
  public boolean isId() {
    return getClass() == SpecificIdAwsRegion.class;
  }

  /** Checks whether the region is specified with a custom non-standard AWS service endpoint. */
  public boolean isCustomEndpoint() {
    return getClass() == CustomEndpointAwsRegion.class;
  }

  /** Returns this region as a {@link SpecificIdAwsRegion}. */
  public SpecificIdAwsRegion asId() {
    if (!isId()) {
      throw new IllegalStateException(
          "This is not an AWS region specified with using the region's unique id.");
    }
    return (SpecificIdAwsRegion) this;
  }

  /** Returns this region as a {@link CustomEndpointAwsRegion}. */
  public CustomEndpointAwsRegion asCustomEndpoint() {
    if (!isCustomEndpoint()) {
      throw new IllegalStateException(
          "This is not an AWS region specified with a custom endpoint.");
    }
    return (CustomEndpointAwsRegion) this;
  }

  public static final class DefaultAwsRegion extends AwsRegion {
    private static final DefaultAwsRegion INSTANCE = new DefaultAwsRegion();
  }

  public static final class SpecificIdAwsRegion extends AwsRegion {
    private final String regionId;

    SpecificIdAwsRegion(String regionId) {
      this.regionId = Objects.requireNonNull(regionId);
    }

    public String id() {
      return regionId;
    }
  }

  public static final class CustomEndpointAwsRegion extends AwsRegion {
    private final String serviceEndpoint;
    private final String regionId;

    CustomEndpointAwsRegion(String serviceEndpoint, String regionId) {
      this.serviceEndpoint = requireValidEndpoint(serviceEndpoint);
      this.regionId = Objects.requireNonNull(regionId);
    }

    public String serviceEndpoint() {
      return serviceEndpoint;
    }

    public String regionId() {
      return regionId;
    }

    private static String requireValidEndpoint(String serviceEndpoint) {
      Objects.requireNonNull(serviceEndpoint);

      final URI uri = URI.create(serviceEndpoint);
      final String scheme = uri.getScheme();
      if (scheme == null
          || (!scheme.equalsIgnoreCase("https") && !scheme.equalsIgnoreCase("http"))) {
        throw new IllegalArgumentException(
            "Invalid service endpoint url: "
                + serviceEndpoint
                + "; Only custom service endpoints using HTTP or HTTPS are supported");
      }

      return serviceEndpoint;
    }
  }
}
