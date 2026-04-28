// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.testutils.function;

import static org.apache.flink.statefun.testutils.matchers.StatefulFunctionMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.junit.jupiter.api.Test;

/** Simple validation tests of the test harness. */
public class FunctionTestHarnessTest {

  private static final FunctionType UNDER_TEST = new FunctionType("flink", "undertest");

  private static final FunctionType OTHER_FUNCTION = new FunctionType("flink", "function");

  private static final Address CALLER = new Address(OTHER_FUNCTION, "id");

  private static final Address SOME_ADDRESS = new Address(OTHER_FUNCTION, "id2");

  private static final Address SELF_ADDRESS = new Address(UNDER_TEST, "id");

  private static final EgressIdentifier<String> EGRESS =
      new EgressIdentifier<>("flink", "egress", String.class);

  @Test
  public void basicMessageTest() {
    FunctionTestHarness harness =
        FunctionTestHarness.test(ignore -> new BasicFunction(), UNDER_TEST, "id");

    assertThat(harness.invoke(CALLER, "ping"), sent(messagesTo(CALLER, equalTo("pong"))));
  }

  @Test
  public void multiReturnTest() {
    FunctionTestHarness harness =
        FunctionTestHarness.test(ignore -> new MultiResponseFunction(), UNDER_TEST, "id");

    assertThat(
        harness.invoke("hello"),
        sent(
            messagesTo(CALLER, equalTo("a"), equalTo("b")),
            messagesTo(SOME_ADDRESS, equalTo("c"))));
  }

  @Test
  public void selfSentTest() {
    FunctionTestHarness harness =
        FunctionTestHarness.test(ignore -> new SelfResponseFunction(), UNDER_TEST, "id");

    assertThat(harness.invoke("hello"), sent(messagesTo(SELF_ADDRESS, equalTo("world"))));
  }

  @Test
  public void egressTest() {
    FunctionTestHarness harness =
        FunctionTestHarness.test(ignore -> new EgressFunction(), UNDER_TEST, "id");

    assertThat(harness.invoke(CALLER, "ping"), sentNothing());
    assertThat(harness.getEgress(EGRESS), contains(equalTo("pong")));
  }

  @Test
  public void delayedMessageTest() {
    FunctionTestHarness harness =
        FunctionTestHarness.test(ignore -> new DelayedResponse(), UNDER_TEST, "id");

    assertThat(harness.invoke(CALLER, "ping"), sentNothing());
    assertThat(harness.tick(Duration.ofMinutes(1)), sent(messagesTo(CALLER, equalTo("pong"))));
  }

  @Test
  public void asyncMessageTest() {
    FunctionTestHarness harness =
        FunctionTestHarness.test(ignore -> new AsyncOperation(), UNDER_TEST, "id");

    assertThat(harness.invoke(CALLER, "ping"), sent(messagesTo(CALLER, equalTo("pong"))));
  }

  private static class BasicFunction implements StatefulFunction {

    @Override
    public void invoke(Context context, Object input) {
      context.reply("pong");
    }
  }

  private static class MultiResponseFunction implements StatefulFunction {

    @Override
    public void invoke(Context context, Object input) {
      context.send(CALLER, "a");
      context.send(CALLER, "b");
      context.send(SOME_ADDRESS, "c");
    }
  }

  private static class SelfResponseFunction implements StatefulFunction {

    @Override
    public void invoke(Context context, Object input) {
      if ("hello".equals(input)) {
        context.send(context.self(), "world");
      }
    }
  }

  private static class EgressFunction implements StatefulFunction {
    @Override
    public void invoke(Context context, Object input) {
      context.send(EGRESS, "pong");
    }
  }

  private static class DelayedResponse implements StatefulFunction {

    @Override
    public void invoke(Context context, Object input) {
      context.sendAfter(Duration.ofMinutes(1), context.caller(), "pong");
    }
  }

  private static class AsyncOperation implements StatefulFunction {

    @Override
    public void invoke(Context context, Object input) {
      if (input instanceof String) {
        CompletableFuture<String> future = CompletableFuture.completedFuture("pong");
        context.registerAsyncOperation(context.caller(), future);
      }

      if (input instanceof AsyncOperationResult) {
        AsyncOperationResult<Address, String> result =
            (AsyncOperationResult<Address, String>) input;
        context.send(result.metadata(), result.value());
      }
    }
  }
}
