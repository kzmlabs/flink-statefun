// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.state.processor;

import java.io.Serializable;
import javax.annotation.Nonnull;
import org.apache.flink.statefun.sdk.io.Router;

/**
 * Provides instances of a {@link Router} to route bootstrap data to {@link
 * StateBootstrapFunction}s.
 *
 * @param <T> data type of elements in the bootstrap dataset being routed.
 */
public interface BootstrapDataRouterProvider<T> extends Serializable {

  /**
   * Creates a {@link Router} instance.
   *
   * @return a router for bootstrap data
   */
  @Nonnull
  Router<T> provide();
}
