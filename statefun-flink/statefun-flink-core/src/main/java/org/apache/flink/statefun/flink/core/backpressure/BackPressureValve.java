// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.backpressure;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.Address;

public interface BackPressureValve {

  /**
   * Indicates rather a back pressure is needed.
   *
   * @return true if a back pressure should be applied.
   */
  boolean shouldBackPressure();

  /**
   * Notifies the back pressure mechanism that a async operation was registered via {@link
   * org.apache.flink.statefun.sdk.Context#registerAsyncOperation(Object, CompletableFuture)}.
   */
  void notifyAsyncOperationRegistered();

  /**
   * Notifies when a async operation, registered by @owningAddress was completed.
   *
   * @param owningAddress the owner of the completed async operation.
   */
  void notifyAsyncOperationCompleted(Address owningAddress);

  /**
   * Requests to stop processing any further input for that address, as long as there is an
   * uncompleted async operation (registered by @address).
   *
   * <p>NOTE: The address would unblocked as soon as some (one) async operation registered by that
   * address completes.
   *
   * @param address the address
   */
  void blockAddress(Address address);

  /**
   * Checks whether a given address was previously blocked with {@link #blockAddress(Address)}.
   *
   * @param address the address to check
   * @return boolean indicating whether or not the address was blocked.
   */
  boolean isAddressBlocked(Address address);
}
