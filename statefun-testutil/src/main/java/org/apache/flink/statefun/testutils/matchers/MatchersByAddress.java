// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.testutils.matchers;

import java.util.List;
import java.util.Objects;
import org.apache.flink.statefun.sdk.Address;
import org.hamcrest.Matcher;

@SuppressWarnings("WeakerAccess")
public class MatchersByAddress {

  final Address address;

  final List<Matcher<?>> matchers;

  MatchersByAddress(Address address, List<Matcher<?>> messages) {
    this.address = Objects.requireNonNull(address);
    this.matchers = Objects.requireNonNull(messages);
  }
}
