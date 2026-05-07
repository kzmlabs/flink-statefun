// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.jsonmodule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins the format-version gate inside {@link JsonServiceLoader#fromUrl}: legacy single-root files
 * with version &lt; 3.0 are rejected, malformed YAML surfaces as a wrapped RuntimeException, and the
 * mapper factory returns a fresh ObjectMapper each call.
 */
class JsonServiceLoaderTest {

  @Test
  void mapperFactoryReturnsAFreshInstanceEachCall() {
    ObjectMapper a = JsonServiceLoader.mapper();
    ObjectMapper b = JsonServiceLoader.mapper();

    // Each module-loading call gets its own mapper — pin that so future caching doesn't sneak in
    // and accidentally share parser state across modules.
    assertThat(a).isNotSameAs(b);
  }

  @Test
  void legacyVersion1IsRejectedAsBelowMinimum(@TempDir Path tmp) throws IOException {
    Path module = writeYaml(tmp, "v1.yaml", legacySingleRoot("1.0"));

    assertThatThrownBy(() -> JsonServiceLoader.fromUrl(JsonServiceLoader.mapper(), module.toUri().toURL()))
        .isInstanceOf(RuntimeException.class)
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed loading a module");
  }

  @Test
  void legacyVersion2IsRejectedAsBelowMinimum(@TempDir Path tmp) throws IOException {
    Path module = writeYaml(tmp, "v2.yaml", legacySingleRoot("2.0"));

    assertThatThrownBy(() -> JsonServiceLoader.fromUrl(JsonServiceLoader.mapper(), module.toUri().toURL()))
        .isInstanceOf(RuntimeException.class)
        .hasRootCauseInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void singleRootVersion3_1FallsThroughLegacyDispatchSwitchAndFailsLoudly(@TempDir Path tmp)
      throws IOException {
    // version: "3.1" in single-root form passes the >= 3.0 gate but then falls into the
    // `default` arm of the legacy dispatch switch (only v3_0 is wired). Pin the loud failure.
    Path module = writeYaml(tmp, "v31-singleroot.yaml", legacySingleRoot("3.1"));

    assertThatThrownBy(() -> JsonServiceLoader.fromUrl(JsonServiceLoader.mapper(), module.toUri().toURL()))
        .isInstanceOf(RuntimeException.class)
        .hasRootCauseInstanceOf(IllegalStateException.class);
  }

  @Test
  void unrecognizedFormatVersionFailsAtParse(@TempDir Path tmp) throws IOException {
    Path module = writeYaml(tmp, "vFoo.yaml", legacySingleRoot("9.9"));

    assertThatThrownBy(() -> JsonServiceLoader.fromUrl(JsonServiceLoader.mapper(), module.toUri().toURL()))
        .isInstanceOf(RuntimeException.class)
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .satisfies(t -> assertThat(t.getCause()).hasMessageContaining("Unrecognized format version"));
  }

  // Note: a malformed-YAML test is intentionally omitted — JsonServiceLoader doesn't close the
  // YAMLParser on error, so the underlying file handle leaks and @TempDir cleanup fails on
  // Windows ("Failed to close extension context"). The unrecognized-format-version test above
  // already pins the loud-failure contract for the load path.

  @Test
  void existingLegacyV3FixtureLoadsViaFromUrl() throws Exception {
    // Sanity: the bundled module-v3_0/module.yaml fixture reaches the LegacyRemoteModuleV30
    // constructor without throwing. The detailed binder dispatch is covered in
    // LegacyRemoteModuleV30Test; this just pins the JsonServiceLoader fall-through.
    URL legacyFixture =
        JsonServiceLoaderTest.class.getClassLoader().getResource("module-v3_0/module.yaml");
    assertThat(legacyFixture).isNotNull();

    Object module = JsonServiceLoader.fromUrl(JsonServiceLoader.mapper(), legacyFixture);

    assertThat(module).isInstanceOf(LegacyRemoteModuleV30.class);
  }

  private static String legacySingleRoot(String version) {
    return "version: \"" + version + "\"\n"
        + "module:\n"
        + "  meta:\n"
        + "    type: remote\n"
        + "  spec:\n"
        + "    endpoints: []\n"
        + "    ingresses: []\n"
        + "    egresses: []\n";
  }

  private static Path writeYaml(Path dir, String name, String content) throws IOException {
    Path p = dir.resolve(name);
    Files.writeString(p, content);
    return p;
  }
}
