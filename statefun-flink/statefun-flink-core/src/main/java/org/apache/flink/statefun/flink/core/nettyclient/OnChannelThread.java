// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.nettyclient;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A Thread that is assigned to a specific Netty Channel. In Netty's programming model, each channel
 * is assigned to a specific thread for the lifetime of the channel, and all Channel releated events
 * are dispatched on this thread.
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface OnChannelThread {}
