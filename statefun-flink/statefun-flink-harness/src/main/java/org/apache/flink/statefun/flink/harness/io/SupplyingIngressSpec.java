// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.harness.io;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;

public final class SupplyingIngressSpec<T> implements IngressSpec<T>, Serializable {

  private static final long serialVersionUID = 1;

  private final IngressIdentifier<T> id;
  private final SerializableSupplier<T> supplier;
  private final long delayInMilliseconds;

  public SupplyingIngressSpec(
      IngressIdentifier<T> id,
      SerializableSupplier<T> supplier,
      long productionDelayInMilliseconds) {
    this.id = Objects.requireNonNull(id);
    this.supplier = Objects.requireNonNull(supplier);
    this.delayInMilliseconds = productionDelayInMilliseconds;
  }

  @Override
  public IngressIdentifier<T> id() {
    return id;
  }

  @Override
  public IngressType type() {
    return HarnessConstants.SUPPLYING_INGRESS_TYPE;
  }

  SerializableSupplier<T> supplier() {
    return supplier;
  }

  long delayInMilliseconds() {
    return delayInMilliseconds;
  }
}
