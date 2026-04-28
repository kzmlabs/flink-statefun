// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.state.processor;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

public class StatefulFunctionsSavepointCreatorTest {

  @Test
  public void invalidMaxParallelism() {
    assertThrows(IllegalArgumentException.class, () -> new StatefulFunctionsSavepointCreator(-1));
  }

  @Test
  public void duplicateStateBootstrapFunctionProvider() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          final StatefulFunctionsSavepointCreator testCreator =
              new StatefulFunctionsSavepointCreator(1);

          testCreator.withStateBootstrapFunctionProvider(
              new FunctionType("ns", "test"), ignored -> new NoOpStateBootstrapFunction());
          testCreator.withStateBootstrapFunctionProvider(
              new FunctionType("ns", "test"), ignored -> new NoOpStateBootstrapFunction());
        });
  }

  @Test
  public void noBootstrapDataOnWrite() {
    assertThrows(
        IllegalStateException.class,
        () -> {
          final StatefulFunctionsSavepointCreator testCreator =
              new StatefulFunctionsSavepointCreator(1);

          testCreator.withStateBootstrapFunctionProvider(
              new FunctionType("ns", "test"), ignored -> new NoOpStateBootstrapFunction());
          testCreator.write("ignored");
        });
  }

  @Test
  public void noStateBootstrapFunctionProvidersOnWrite() {
    assertThrows(
        IllegalStateException.class,
        () -> {
          final StreamExecutionEnvironment env =
              StreamExecutionEnvironment.getExecutionEnvironment();
          final StatefulFunctionsSavepointCreator testCreator =
              new StatefulFunctionsSavepointCreator(1);

          testCreator.withBootstrapData(env.fromElements("foobar"), NoOpBootstrapDataRouter::new);
          testCreator.write("ignored");
        });
  }

  private static class NoOpStateBootstrapFunction implements StateBootstrapFunction {
    @Override
    public void bootstrap(Context context, Object bootstrapData) {}
  }

  private static class NoOpBootstrapDataRouter<T> implements Router<T> {
    @Override
    public void route(T message, Downstream<T> downstream) {}
  }
}
