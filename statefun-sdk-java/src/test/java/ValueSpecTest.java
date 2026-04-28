// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.junit.jupiter.api.Test;

public class ValueSpecTest {

  @Test
  public void exampleUsage() {
    final ValueSpec<Integer> spec = ValueSpec.named("state_name").withIntType();

    assertThat(spec.name(), is("state_name"));
    assertThat(spec.type(), is(Types.integerType()));
  }

  @Test
  public void stateNameWithSpaces() {
    assertThrows(
        IllegalArgumentException.class, () -> ValueSpec.named("bad state name").withIntType());
  }

  @Test
  public void stateNameWithInvalidStartChar() {
    assertThrows(
        IllegalArgumentException.class, () -> ValueSpec.named("123bad_state_name").withIntType());
  }

  @Test
  public void stateNameWithInvalidPartChar() {
    assertThrows(
        IllegalArgumentException.class, () -> ValueSpec.named("bad!_state_name").withIntType());
  }
}
