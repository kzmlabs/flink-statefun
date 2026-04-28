// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.extensions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

/**
 * A {@link ComponentBinder} binds {@link ComponentJsonObject}s to a remote module. It parses the
 * specifications of a given component, resolves them into application entities, such as function
 * providers, ingresses, or egresses, and then binds the entities to the module.
 */
@PublicEvolving
public interface ComponentBinder {

  /**
   * Bind a {@link ComponentJsonObject} to an underlying remote module through the provided module
   * binder.
   *
   * @param component the component to parse and bind.
   * @param remoteModuleBinder the binder to use to bind application entities to the underlying
   *     remote module.
   */
  void bind(ComponentJsonObject component, StatefulFunctionModule.Binder remoteModuleBinder);
}
