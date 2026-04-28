// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.spi;

import org.apache.flink.statefun.extensions.ExtensionModule;
import org.apache.flink.statefun.sdk.TypeName;

/**
 * Resolves a bound extension (bound by {@link ExtensionModule}s) given specified {@link TypeName}s.
 */
public interface ExtensionResolver {
  <T> T resolveExtension(TypeName typeName, Class<T> extensionClass);
}
