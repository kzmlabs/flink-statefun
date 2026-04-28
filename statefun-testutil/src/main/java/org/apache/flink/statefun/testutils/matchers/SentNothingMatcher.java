// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.testutils.matchers;

import java.util.List;
import java.util.Map;
import org.apache.flink.statefun.sdk.Address;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/** A matcher for that the function did not message any other functions. */
public class SentNothingMatcher extends TypeSafeMatcher<Map<Address, List<Object>>> {

  SentNothingMatcher() {}

  @Override
  protected boolean matchesSafely(Map<Address, List<Object>> item) {
    return item.isEmpty();
  }

  @Override
  public void describeTo(Description description) {
    description.appendText("Nothing Sent");
  }
}
