// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.state.processor;

import org.apache.flink.statefun.sdk.Address;

/** Provides context for a single {@link StateBootstrapFunction} invocation. */
public interface Context {

  /**
   * Returns the {@link Address} of the function being bootstrapped.
   *
   * @return the address of the function being bootstrapped.
   */
  Address self();
}
