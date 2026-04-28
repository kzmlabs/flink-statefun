// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.message;

import javax.annotation.Nullable;
import org.apache.flink.statefun.sdk.Address;

/** A message with source and target {@link Address}s. */
public interface RoutableMessage {

  /**
   * Gets the address of the sender.
   *
   * @return the address (optinal) address of the sender.
   */
  @Nullable
  Address source();

  /**
   * Gets the target address.
   *
   * @return the target address that this message is designated to.
   */
  Address target();
}
