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

    assertThatThrownBy(() -> ModuleSpecs.fromPath(file.toString()))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("is not a directory");
  }

  @Test
  void fromPathOnEmptyDirectoryReturnsEmptyModuleList(@TempDir Path tmp) throws Exception {
    ModuleSpecs specs = ModuleSpecs.fromPath(tmp.toString());

    assertThat(specs.modules()).isEmpty();
    assertThat(specs.iterator().hasNext()).isFalse();
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
  void fromCollectionPreservesOrderAndContent() throws Exception {
    File f1 = File.createTempFile("statefun-test", ".jar");
    f1.deleteOnExit();
    ModuleSpecs.ModuleSpec spec =
        moduleSpecBuilder().withJarFile(f1).build();

    ModuleSpecs specs = ModuleSpecs.fromCollection(spec);

    assertThat(specs.modules()).containsExactly(spec);
  }

  @Test
  void moduleSpecBuilderRejectsNullJarFile() {
    assertThatThrownBy(() -> moduleSpecBuilder().withJarFile(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void moduleSpecBuilderRejectsNullYamlFile() {
    assertThatThrownBy(() -> moduleSpecBuilder().withYamlModuleFile(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void moduleSpecBuilderRejectsNullUri() {
    assertThatThrownBy(() -> moduleSpecBuilder().withUri(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void moduleSpecArtifactUrisAreSortedAndUnmodifiable() throws Exception {
    File f2 = File.createTempFile("statefun-z", ".jar");
    File f1 = File.createTempFile("statefun-a", ".jar");
    f1.deleteOnExit();
    f2.deleteOnExit();
    // Add in reverse order; the TreeSet inside the builder should produce a sorted list.
    ModuleSpecs.ModuleSpec spec = moduleSpecBuilder().withJarFile(f2).withJarFile(f1).build();

    assertThat(spec.artifactUris())
        .hasSize(2)
        .isSortedAccordingTo(java.net.URI::compareTo);
    // Unmodifiable
    assertThatThrownBy(() -> spec.artifactUris().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  // Bridge to the package-private builder.
  private static ModuleSpecs.ModuleSpec.Builder moduleSpecBuilder() {
    try {
      java.lang.reflect.Method m = ModuleSpecs.ModuleSpec.class.getDeclaredMethod("builder");
      m.setAccessible(true);
      return (ModuleSpecs.ModuleSpec.Builder) m.invoke(null);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
