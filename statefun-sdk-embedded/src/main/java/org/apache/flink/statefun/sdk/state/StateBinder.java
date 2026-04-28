// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.sdk.state;

import org.apache.flink.statefun.sdk.annotations.ForRuntime;

@ForRuntime
public abstract class StateBinder {
  public abstract void bind(Object stateObject);
}
