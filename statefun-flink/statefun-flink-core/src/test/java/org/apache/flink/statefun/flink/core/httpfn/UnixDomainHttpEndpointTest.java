// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.httpfn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.net.URI;
import org.junit.jupiter.api.Test;

public class UnixDomainHttpEndpointTest {

  @Test
  public void splitOnlyWithFile() {
    UnixDomainHttpEndpoint out =
        UnixDomainHttpEndpoint.parseFrom(URI.create("http+unix:///some/path.sock"));

    // Compare using File to handle platform-specific path separators
    assertEquals(new File("/some/path.sock"), out.unixDomainFile);
    assertEquals("/", out.pathSegment);
  }

  @Test
  public void splitOnlyWithFileAndEndpoint() {
    UnixDomainHttpEndpoint out =
        UnixDomainHttpEndpoint.parseFrom(URI.create("http+unix:///some/path.sock/hello"));

    // Compare using File to handle platform-specific path separators
    assertEquals(new File("/some/path.sock"), out.unixDomainFile);
    assertEquals("/hello", out.pathSegment);
  }

  @Test
  public void missingSockFile() {
    assertThrows(
        IllegalStateException.class,
        () -> UnixDomainHttpEndpoint.parseFrom(URI.create("http+unix:///some/path/hello")));
  }

  @Test
  public void validateUdsEndpoint() {
    assertFalse(UnixDomainHttpEndpoint.validate(URI.create("http:///bar.foo.com/some/path")));
  }

  @Test
  public void parseNonUdsEndpoint() {
    assertThrows(
        IllegalArgumentException.class,
        () -> UnixDomainHttpEndpoint.parseFrom(URI.create("http:///bar.foo.com/some/path")));
  }
}
