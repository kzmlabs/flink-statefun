// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.io;

import org.apache.flink.statefun.sdk.EgressType;

/**
 * Complete specification for an egress, containing of the egress' {@link EgressIdentifier} and the
 * {@link EgressType}. This fully defines an egress within a Stateful Functions application.
 *
 * <p>This serves as a "logical" representation of an output sink that stateful functions within an
 * application can send messages to. Under the scenes, the system translates this to a physical
 * runtime-specific representation corresponding to the specified {@link EgressType}.
 *
 * @param <T> the type of messages consumed by this egress.
 */
public interface EgressSpec<T> {

  /**
   * Returns the unique identifier of the egress.
   *
   * @return the unique identifier of the egress.
   */
  EgressIdentifier<T> id();

  /**
   * Returns the type of the egress.
   *
   * @return the type of the egress.
   */
  EgressType type();
}
