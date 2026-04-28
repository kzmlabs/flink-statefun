// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.java;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.junit.jupiter.api.Test;

public class StatefulFunctionSpecTest {

  private static final ValueSpec<Integer> STATE_A = ValueSpec.named("state_a").withIntType();
  private static final ValueSpec<Boolean> STATE_B = ValueSpec.named("state_b").withBooleanType();

  @Test
  public void exampleUsage() {
    final StatefulFunctionSpec spec =
        StatefulFunctionSpec.builder(TypeName.typeNameOf("test.namespace", "test.name"))
            .withValueSpecs(STATE_A, STATE_B)
            .withSupplier(TestFunction::new)
            .build();

    assertThat(spec.supplier().get(), instanceOf(TestFunction.class));
    assertThat(spec.typeName(), is(TypeName.typeNameOf("test.namespace", "test.name")));
    assertThat(spec.knownValues(), hasKey("state_a"));
    assertThat(spec.knownValues(), hasKey("state_b"));
  }

  @Test
  public void duplicateRegistration() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StatefulFunctionSpec.builder(TypeName.typeNameOf("test.namespace", "test.name"))
                .withValueSpecs(
                    ValueSpec.named("foobar").withIntType(),
                    ValueSpec.named("foobar").withBooleanType()));
  }

  private static class TestFunction implements StatefulFunction {
    @Override
    public CompletableFuture<Void> apply(Context context, Message argument) {
      // no-op
      return context.done();
    }
  }
}
