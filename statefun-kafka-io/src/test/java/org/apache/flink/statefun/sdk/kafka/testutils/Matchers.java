// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.kafka.testutils;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public final class Matchers {

  private Matchers() {}

  public static <K, V> Matcher<Map<K, V>> isMapOfSize(int size) {
    return new TypeSafeMatcher<Map<K, V>>() {
      @Override
      protected boolean matchesSafely(Map<K, V> map) {
        return map.size() == size;
      }

      @Override
      public void describeTo(Description description) {}
    };
  }

  public static Matcher<Properties> hasProperty(String key, String value) {
    return new TypeSafeMatcher<Properties>() {
      @Override
      protected boolean matchesSafely(Properties properties) {
        return Objects.equals(properties.getProperty(key), value);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("<" + key + "=" + value + ">");
      }
    };
  }
}
