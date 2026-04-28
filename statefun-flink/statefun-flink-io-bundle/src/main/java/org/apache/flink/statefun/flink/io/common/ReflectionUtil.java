// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.common;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.flink.annotation.Internal;

@Internal
public final class ReflectionUtil {

  private ReflectionUtil() {}

  public static <T> T instantiate(Class<T> type) {
    try {
      Constructor<T> defaultConstructor = type.getDeclaredConstructor();
      defaultConstructor.setAccessible(true);
      return defaultConstructor.newInstance();
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(
          "Unable to create an instance of " + type.getName() + " has no default constructor", e);
    } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
      throw new IllegalStateException("Unable to create an instance of " + type.getName(), e);
    }
  }
}
