// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.message;

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;

/** A {@link RoutableMessage} Builder. */
public final class RoutableMessageBuilder {

  public static RoutableMessageBuilder builder() {
    return new RoutableMessageBuilder();
  }

  @Nullable private Address source;
  private Address target;
  private Object payload;

  private RoutableMessageBuilder() {}

  public RoutableMessageBuilder withTargetAddress(FunctionType functionType, String id) {
    return withTargetAddress(new Address(functionType, id));
  }

  public RoutableMessageBuilder withTargetAddress(Address target) {
    this.target = Objects.requireNonNull(target);
    return this;
  }

  public RoutableMessageBuilder withSourceAddress(FunctionType functionType, String id) {
    return withSourceAddress(new Address(functionType, id));
  }

  public RoutableMessageBuilder withSourceAddress(@Nullable Address from) {
    this.source = from;
    return this;
  }

  public RoutableMessageBuilder withMessageBody(Object payload) {
    this.payload = Objects.requireNonNull(payload);
    return this;
  }

  public RoutableMessage build() {
    return new SdkMessage(source, target, payload);
  }
}
