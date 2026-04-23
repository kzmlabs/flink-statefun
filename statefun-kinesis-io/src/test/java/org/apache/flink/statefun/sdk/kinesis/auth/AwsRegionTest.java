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
package org.apache.flink.statefun.sdk.kinesis.auth;

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
}
