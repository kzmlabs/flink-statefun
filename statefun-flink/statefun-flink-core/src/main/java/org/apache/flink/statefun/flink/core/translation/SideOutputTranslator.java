// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.translation;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.types.StaticallyRegisteredTypes;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.util.OutputTag;

final class SideOutputTranslator {
  private final StaticallyRegisteredTypes types;
  private final Iterable<EgressIdentifier<?>> egressIdentifiers;

  SideOutputTranslator(StatefulFunctionsUniverse universe) {
    this(universe.types(), universe.egress().keySet());
  }

  SideOutputTranslator(
      StaticallyRegisteredTypes types, Iterable<EgressIdentifier<?>> egressIdentifiers) {
    this.types = types;
    this.egressIdentifiers = egressIdentifiers;
  }

  Map<EgressIdentifier<?>, OutputTag<Object>> translate() {
    Map<EgressIdentifier<?>, OutputTag<Object>> outputTags = new HashMap<>();

    for (EgressIdentifier<?> id : egressIdentifiers) {
      outputTags.put(id, outputTagFromId(id, types));
    }

    return outputTags;
  }

  private static OutputTag<Object> outputTagFromId(
      EgressIdentifier<?> id, StaticallyRegisteredTypes types) {
    @SuppressWarnings("unchecked")
    EgressIdentifier<Object> casted = (EgressIdentifier<Object>) id;
    String name = String.format("%s.%s", id.namespace(), id.name());
    TypeInformation<Object> typeInformation = types.registerType(casted.consumedType());
    return new OutputTag<>(name, typeInformation);
  }
}
