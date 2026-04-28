// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.kinesis.auth;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class AwsRegionTest {

  @Test
  public void ofCustomEndpoint_https_ok() {
    final AwsRegion region =
        AwsRegion.ofCustomEndpoint("https://kinesis.us-east-1.amazonaws.com", "us-east-1");

    assertEquals(
        "https://kinesis.us-east-1.amazonaws.com", region.asCustomEndpoint().serviceEndpoint());
    assertEquals("us-east-1", region.asCustomEndpoint().regionId());
  }

  @Test
  public void ofCustomEndpoint_http_ok() {
    // LocalStack and similar local AWS emulators expose plain HTTP; this must now be accepted.
    final AwsRegion region = AwsRegion.ofCustomEndpoint("http://localhost:4566", "us-east-1");

    assertEquals("http://localhost:4566", region.asCustomEndpoint().serviceEndpoint());
    assertEquals("us-east-1", region.asCustomEndpoint().regionId());
  }

  @Test
  public void ofCustomEndpoint_invalidScheme_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> AwsRegion.ofCustomEndpoint("ftp://some-bucket.example.com", "us-east-1"));
  }

  @Test
  void ofCustomEndpoint_schemelessHost_throws() {
    assertThatThrownBy(() -> AwsRegion.ofCustomEndpoint("localhost:4566", "us-east-1"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
