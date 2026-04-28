// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.httpfn;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.apache.flink.util.Preconditions;

/** Represents a Unix domain file path and an http endpoint */
final class UnixDomainHttpEndpoint {

  /** Checks whether or not an endpoint is using UNIX domain sockets. */
  static boolean validate(URI endpoint) {
    String scheme = endpoint.getScheme();
    return "http+unix".equalsIgnoreCase(scheme) || "https+unix".equalsIgnoreCase(scheme);
  }

  /** Parses a URI of the form {@code http+unix://<file system path>.sock/<http endpoint>}. */
  static UnixDomainHttpEndpoint parseFrom(URI endpoint) {
    Preconditions.checkArgument(validate(endpoint));
    final Path path = Paths.get(endpoint.getPath());
    final int sockPathIndex = indexOfSockFile(path);
    final String filePath = "/" + path.subpath(0, sockPathIndex + 1).toString();
    final File unixDomainFile = new File(filePath);

    if (sockPathIndex == path.getNameCount() - 1) {
      return new UnixDomainHttpEndpoint(unixDomainFile, "/");
    }
    String pathSegment = "/" + path.subpath(sockPathIndex + 1, path.getNameCount()).toString();
    return new UnixDomainHttpEndpoint(unixDomainFile, pathSegment);
  }

  private static int indexOfSockFile(Path path) {
    for (int i = 0; i < path.getNameCount(); i++) {
      if (path.getName(i).toString().endsWith(".sock")) {
        return i;
      }
    }
    throw new IllegalStateException("Unix Domain Socket path should contain a .sock file");
  }

  final File unixDomainFile;
  final String pathSegment;

  private UnixDomainHttpEndpoint(File unixDomainFile, String endpoint) {
    this.unixDomainFile = Objects.requireNonNull(unixDomainFile);
    this.pathSegment = Objects.requireNonNull(endpoint);
  }
}
