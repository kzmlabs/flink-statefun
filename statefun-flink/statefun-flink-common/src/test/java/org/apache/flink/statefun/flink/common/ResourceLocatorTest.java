// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ResourceLocatorTest {

  static Stream<Configuration> filesystemTypes() {
    return Stream.of(Configuration.unix(), Configuration.osX(), Configuration.windows());
  }

  @ParameterizedTest
  @MethodSource("filesystemTypes")
  void classPathExample(Configuration filesystemConfiguration) throws IOException {
    FileSystem fileSystem = Jimfs.newFileSystem(filesystemConfiguration);
    final Path firstModuleDir = createDirectoryWithAFile(fileSystem, "first", "module.yaml");
    final Path secondModuleDir = createDirectoryWithAFile(fileSystem, "second", "module.yaml");

    ClassLoader urlClassLoader = urlClassLoader(firstModuleDir, secondModuleDir);

    try (SetContextClassLoader ignored = new SetContextClassLoader(urlClassLoader)) {

      Iterable<URL> foundUrls = ResourceLocator.findNamedResources("classpath:module.yaml");

      assertThat(
          foundUrls,
          contains(
              url(firstModuleDir.resolve("module.yaml")),
              url(secondModuleDir.resolve("module.yaml"))));
    }
  }

  @Test
  void classPathSingleResourceExample() {
    URL url = ResourceLocator.findNamedResource("classpath:dummy-file.txt");

    assertThat(url, notNullValue());
  }

  @ParameterizedTest
  @MethodSource("filesystemTypes")
  void absolutePathExample(Configuration filesystemConfiguration) throws IOException {
    FileSystem fileSystem = Jimfs.newFileSystem(filesystemConfiguration);
    Path modulePath =
        createDirectoryWithAFile(fileSystem, "some-module", "module.yaml").resolve("module.yaml");

    URL url = ResourceLocator.findNamedResource(modulePath.toUri().toString());

    assertThat(url, is(url(modulePath)));
  }

  @Test
  void nonAbosultePath() throws MalformedURLException {
    URL url = ResourceLocator.findNamedResource("/tmp/a.txt");

    assertThat(url, is(url("file:/tmp/a.txt")));
  }

  private URL url(@SuppressWarnings("SameParameterValue") String url) throws MalformedURLException {
    return URI.create(url).toURL();
  }

  private static Path createDirectoryWithAFile(
      FileSystem fileSystem,
      String basedir,
      @SuppressWarnings("SameParameterValue") String filename)
      throws IOException {
    final Path dir = fileSystem.getPath(basedir);
    Files.createDirectories(dir);

    Path file = dir.resolve(filename);
    Files.write(file, "hello world".getBytes(StandardCharsets.UTF_8));

    return dir;
  }

  private static ClassLoader urlClassLoader(Path... urlPath) {
    URL[] urls = Arrays.stream(urlPath).map(ResourceLocatorTest::url).toArray(URL[]::new);
    return new URLClassLoader(urls);
  }

  private static URL url(Path path) {
    try {
      return path.toUri().toURL();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }
}
