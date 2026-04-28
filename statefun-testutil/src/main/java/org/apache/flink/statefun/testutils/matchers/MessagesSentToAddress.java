// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.testutils.matchers;

import java.util.List;
import java.util.Map;
import org.apache.flink.statefun.sdk.Address;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/** A matcher for checking all the responses sent to a particular function type. */
public class MessagesSentToAddress extends TypeSafeMatcher<Map<Address, List<Object>>> {

  private final Map<Address, List<Matcher<?>>> matcherByAddress;

  MessagesSentToAddress(Map<Address, List<Matcher<?>>> matcherByAddress) {
    this.matcherByAddress = matcherByAddress;
  }

  @Override
  protected boolean matchesSafely(Map<Address, List<Object>> item) {
    for (Map.Entry<Address, List<Matcher<?>>> entry : matcherByAddress.entrySet()) {
      List<Object> messages = item.get(entry.getKey());
      if (messages == null) {
        return false;
      }

      if (messages.size() != entry.getValue().size()) {
        return false;
      }

      for (int i = 0; i < messages.size(); i++) {
        Matcher<?> matcher = entry.getValue().get(i);
        if (!matcher.matches(messages.get(i))) {
          return false;
        }
      }
    }

    return true;
  }

  @Override
  public void describeTo(Description description) {
    description.appendText("<{");

    for (Map.Entry<Address, List<Matcher<?>>> entry : matcherByAddress.entrySet()) {
      description
          .appendText(entry.getKey().toString())
          .appendText("=")
          .appendList("[", ",", "]", entry.getValue());
    }

    description.appendText("}>");
  }
}
