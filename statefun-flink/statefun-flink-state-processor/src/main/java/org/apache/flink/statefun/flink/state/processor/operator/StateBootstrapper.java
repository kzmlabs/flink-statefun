// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.state.processor.operator;

import java.util.Objects;
import org.apache.flink.statefun.flink.core.state.State;
import org.apache.flink.statefun.flink.state.processor.Context;
import org.apache.flink.statefun.flink.state.processor.StateBootstrapFunction;
import org.apache.flink.statefun.flink.state.processor.union.TaggedBootstrapData;
import org.apache.flink.statefun.sdk.Address;

/** Core logic for bootstrapping function state using user-provided state bootstrap functions. */
final class StateBootstrapper {

  private final StateBootstrapFunctionRegistry bootstrapFunctionRegistry;
  private final State stateAccessor;
  private final ReusableContext stateBootstrapFunctionContext;

  StateBootstrapper(StateBootstrapFunctionRegistry bootstrapFunctionRegistry, State stateAccessor) {
    this.bootstrapFunctionRegistry = Objects.requireNonNull(bootstrapFunctionRegistry);
    this.stateAccessor = Objects.requireNonNull(stateAccessor);
    this.stateBootstrapFunctionContext = new ReusableContext();

    bootstrapFunctionRegistry.initialize(stateAccessor);
  }

  void apply(TaggedBootstrapData bootstrapData) {
    final Address target = bootstrapData.getTarget();

    stateAccessor.setCurrentKey(target);
    stateBootstrapFunctionContext.setCurrentAddress(target);

    final StateBootstrapFunction bootstrapFunction =
        bootstrapFunctionRegistry.getBootstrapFunction(target.type());
    if (bootstrapFunction == null) {
      throw new IllegalArgumentException(
          "A bootstrap input was targeted for function of type "
              + target.type()
              + ", but there was no StateBootstrapFunctionProvider registered for the type.");
    }

    bootstrapFunction.bootstrap(stateBootstrapFunctionContext, bootstrapData.getPayload());
  }

  private static class ReusableContext implements Context {

    private Address self = null;

    @Override
    public Address self() {
      if (self == null) {
        throw new IllegalStateException("Current address is not set.");
      }
      return self;
    }

    private void setCurrentAddress(Address currentAddress) {
      this.self = currentAddress;
    }
  }
}
