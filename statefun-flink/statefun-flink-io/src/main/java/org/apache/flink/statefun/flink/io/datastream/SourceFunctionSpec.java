// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.io.datastream;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;

/**
 * An {@link IngressSpec} that can run any Apache Flink {@link Source}.
 *
 * @param <T> The input type consumed by the source.
 */
public final class SourceFunctionSpec<T, SplitT extends SourceSplit, EnumChckT>
    implements IngressSpec<T>, Serializable {
  private static final long serialVersionUID = 1;

  static final IngressType TYPE =
      new IngressType("org.apache.flink.statefun.flink.io", "source-function-spec");

  private final IngressIdentifier<T> id;
  private final Source<T, SplitT, EnumChckT> delegate;

  /**
   * @param id A unique ingress identifier.
   * @param delegate The underlying source function that this spec will delegate to at runtime.
   */
  public SourceFunctionSpec(IngressIdentifier<T> id, Source<T, SplitT, EnumChckT> delegate) {
    this.id = Objects.requireNonNull(id);
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public final IngressIdentifier<T> id() {
    return id;
  }

  @Override
  public final IngressType type() {
    return TYPE;
  }

  Source<T, SplitT, EnumChckT> delegate() {
    return delegate;
  }
}
