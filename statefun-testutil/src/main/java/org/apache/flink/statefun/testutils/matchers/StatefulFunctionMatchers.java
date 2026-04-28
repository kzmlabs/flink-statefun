// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.testutils.matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.testutils.function.FunctionTestHarness;
import org.hamcrest.Matcher;

/**
 * A set of Hamcrest matchers to help check the responses from a {@link FunctionTestHarness}
 *
 * @see FunctionTestHarness for usage details.
 */
public final class StatefulFunctionMatchers {

  private StatefulFunctionMatchers() {
    throw new AssertionError();
  }

  public static MatchersByAddress messagesTo(
      Address to, Matcher<?> matcher, Matcher<?>... matchers) {
    List<Matcher<?>> allMatchers = new ArrayList<>(1 + matchers.length);
    allMatchers.add(matcher);
    allMatchers.addAll(Arrays.asList(matchers));

    return new MatchersByAddress(to, allMatchers);
  }

  /**
   * A matcher that checks all the responses sent to a given {@link FunctionType}.
   *
   * <p><b>Important:</b> This matcher expects an exact match on the number of responses sent to
   * this function.
   *
   * @param matcher matcher for address.
   * @param matchers matchers for addresses.
   * @return a matcher that checks all the responses sent to a given {@link FunctionType}.
   */
  public static MessagesSentToAddress sent(
      MatchersByAddress matcher, MatchersByAddress... matchers) {
    Map<Address, List<Matcher<?>>> messagesByAddress = new HashMap<>();
    messagesByAddress.put(matcher.address, matcher.matchers);

    for (MatchersByAddress match : matchers) {
      messagesByAddress.put(match.address, match.matchers);
    }

    return new MessagesSentToAddress(messagesByAddress);
  }

  /**
   * A matcher that checks the function did not send any messages.
   *
   * @return a matcher that checks the function did not send any messages.
   */
  public static SentNothingMatcher sentNothing() {
    return new SentNothingMatcher();
  }
}
