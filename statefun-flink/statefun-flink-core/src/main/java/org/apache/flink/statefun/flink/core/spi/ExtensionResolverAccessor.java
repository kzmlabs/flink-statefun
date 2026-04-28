// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.spi;

import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

/**
 * TODO This is a temporary workaround for accessing the {@link ExtensionResolver}. TODO We should
 * expose the resolver properly once we have more usages.
 */
public final class ExtensionResolverAccessor {
  private ExtensionResolverAccessor() {}

  public static ExtensionResolver getExtensionResolver(StatefulFunctionModule.Binder moduleBinder) {
    // the binder is always the StatefulFunctionsUniverse, which implements ExtensionResolver
    return (ExtensionResolver) moduleBinder;
  }
}
