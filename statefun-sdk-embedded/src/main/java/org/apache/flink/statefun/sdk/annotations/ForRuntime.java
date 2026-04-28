// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * Methods or constructors annotated with this annotation, are used for the runtime to extend the
 * API with specialized implementation
 */
@Documented
@Target({ElementType.CONSTRUCTOR, ElementType.METHOD, ElementType.TYPE})
public @interface ForRuntime {}
