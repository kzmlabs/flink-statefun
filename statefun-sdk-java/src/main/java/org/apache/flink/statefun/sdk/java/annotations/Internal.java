// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.java.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * Methods, constructors or classes annotated with this annotation, are used for by the SDK
 * internally.
 */
@Documented
@Target({ElementType.CONSTRUCTOR, ElementType.METHOD, ElementType.TYPE})
public @interface Internal {}
