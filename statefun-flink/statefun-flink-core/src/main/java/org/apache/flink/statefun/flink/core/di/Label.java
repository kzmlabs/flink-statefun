// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.di;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface Label {
  String value();
}
