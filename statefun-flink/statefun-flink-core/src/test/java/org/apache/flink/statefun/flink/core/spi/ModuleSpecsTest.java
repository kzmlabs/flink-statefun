// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ModuleSpecsTest {

  @Test
  void fromPathOnNonExistentDirectoryThrowsIllegalArgument(@TempDir Path tmp) {
    File ghost = new File(tmp.toFile(), "does-not-exist");

    assertThatThrownBy(() -> ModuleSpecs.fromPath(ghost.getAbsolutePath()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not exists");
  }

  @Test
  void fromPathOnFileNotDirectoryThrows(@TempDir Path tmp) throws Exception {
    Path file = Files.createFile(tmp.resolve("a-file"));

    // Production code at ModuleSpecs.discoverLoadableArtifacts:41 throws raw RuntimeException
    // (not IllegalArgumentException) for the "not a directory" path. Match exactly — narrowing
    // here would fail the test, and there is a separate latent issue in production worth tracking
    // (the two failure paths use different exception types).
    assertThatThrownBy(() -> ModuleSpecs.fromPath(file.toString()))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("is not a directory");
  }

  @Test
  void fromPathOnEmptyDirectoryReturnsEmptyModuleList(@TempDir Path tmp) throws Exception {
    ModuleSpecs specs = ModuleSpecs.fromPath(tmp.toString());

    assertThat(specs.modules()).isEmpty();
    assertThat(specs).isEmpty();
  }

  @Test
  void fromPathDiscoversJarsAndYamlModulesInSubdirectories(@TempDir Path tmp) throws Exception {
    // Create two module subdirs, each with a jar + module.yaml — the production layout.
    Path moduleA = Files.createDirectory(tmp.resolve("module-a"));
    Files.createFile(moduleA.resolve("a.jar"));
    Files.createFile(moduleA.resolve("module.yaml"));
    Path moduleB = Files.createDirectory(tmp.resolve("module-b"));
    Files.createFile(moduleB.resolve("b.jar"));

    ModuleSpecs specs = ModuleSpecs.fromPath(tmp.toString());

    assertThat(specs.modules()).hasSize(2);
  }

  @Test
  void fromPathSkipsFilesAtTopLevelAndOnlyRecursesIntoSubdirectories(@TempDir Path tmp)
      throws Exception {
    // Files at top-level should be ignored — only subdirectories are scanned for modules.
    Files.createFile(tmp.resolve("loose.jar"));
    Files.createFile(tmp.resolve("module.yaml"));
    Path moduleA = Files.createDirectory(tmp.resolve("module-a"));
    Files.createFile(moduleA.resolve("a.jar"));

    ModuleSpecs specs = ModuleSpecs.fromPath(tmp.toString());

    assertThat(specs.modules()).hasSize(1);
    assertThat(specs.modules().get(0).artifactUris()).hasSize(1);
  }

  @Test
  void fromCollectionPreservesOrderAndContent(@TempDir Path tmp) throws Exception {
    Path f1 = Files.createFile(tmp.resolve("statefun-test.jar"));
    ModuleSpecs.ModuleSpec spec =
        ModuleSpecs.ModuleSpec.builder().withJarFile(f1.toFile()).build();

    ModuleSpecs specs = ModuleSpecs.fromCollection(spec);

    assertThat(specs.modules()).containsExactly(spec);
  }

  @Test
  void moduleSpecBuilderRejectsNullJarFile() {
    assertThatThrownBy(() -> ModuleSpecs.ModuleSpec.builder().withJarFile(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void moduleSpecBuilderRejectsNullYamlFile() {
    assertThatThrownBy(() -> ModuleSpecs.ModuleSpec.builder().withYamlModuleFile(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void moduleSpecBuilderRejectsNullUri() {
    assertThatThrownBy(() -> ModuleSpecs.ModuleSpec.builder().withUri(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void moduleSpecArtifactUrisAreSortedAndUnmodifiable(@TempDir Path tmp) throws Exception {
    Path f2 = Files.createFile(tmp.resolve("statefun-z.jar"));
    Path f1 = Files.createFile(tmp.resolve("statefun-a.jar"));
    // Add in reverse order; the TreeSet inside the builder should produce a sorted list.
    ModuleSpecs.ModuleSpec spec =
        ModuleSpecs.ModuleSpec.builder().withJarFile(f2.toFile()).withJarFile(f1.toFile()).build();

    assertThat(spec.artifactUris())
        .hasSize(2)
        .isSortedAccordingTo(java.net.URI::compareTo);
    // Unmodifiable
    assertThatThrownBy(() -> spec.artifactUris().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
