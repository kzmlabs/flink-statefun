// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsSame.sameInstance;

import org.apache.flink.statefun.flink.core.message.MessageFactoryKey;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.sdk.TypeName;
import org.junit.jupiter.api.Test;

public class StatefulFunctionsUniverseTest {

  @Test
  public void testExtensions() {
    final StatefulFunctionsUniverse universe = emptyUniverse();

    final ExtensionImpl extension = new ExtensionImpl();
    universe.bindExtension(TypeName.parseFrom("test.namespace/test.name"), extension);

    assertThat(
        extension,
        sameInstance(
            universe.resolveExtension(
                TypeName.parseFrom("test.namespace/test.name"), BaseExtension.class)));
  }

  private static StatefulFunctionsUniverse emptyUniverse() {
    return new StatefulFunctionsUniverse(
        MessageFactoryKey.forType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, null));
  }

  private interface BaseExtension {}

  private static class ExtensionImpl implements BaseExtension {}
}
