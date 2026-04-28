// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.sdk.state;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.flink.statefun.sdk.TypeName;
import org.junit.jupiter.api.Test;

public class PersistedStateRegistryTest {

  @Test
  public void exampleUsage() {
    final PersistedStateRegistry registry = new PersistedStateRegistry();

    registry.registerValue(PersistedValue.of("value", String.class));
    registry.registerTable(PersistedTable.of("table", String.class, Integer.class));
    registry.registerAppendingBuffer(PersistedAppendingBuffer.of("buffer", String.class));
    registry.registerRemoteValue(
        RemotePersistedValue.of("remote", TypeName.parseFrom("io.statefun.types/raw")));
  }

  @Test
  public void duplicateRegistration() {
    assertThrows(
        IllegalStateException.class,
        () -> {
          final PersistedStateRegistry registry = new PersistedStateRegistry();

          registry.registerValue(PersistedValue.of("my-state", String.class));
          registry.registerTable(PersistedTable.of("my-state", String.class, Integer.class));
        });
  }
}
