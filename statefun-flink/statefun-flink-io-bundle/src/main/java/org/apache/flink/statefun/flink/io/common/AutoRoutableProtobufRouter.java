// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.io.common;

import com.google.protobuf.Message;
import org.apache.flink.statefun.flink.io.generated.AutoRoutable;
import org.apache.flink.statefun.flink.io.generated.Header;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.flink.io.generated.TargetFunctionType;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

/**
 * A {@link Router} that recognizes messages of type {@link AutoRoutable}.
 *
 * <p>For each incoming {@code AutoRoutable}, this router forwards the wrapped payload to the
 * configured target addresses as a {@link TypedValue} message.
 */
public final class AutoRoutableProtobufRouter implements Router<Message> {

  /**
   * Note: while the input and type of this method is both {@link Message}, we actually do a
   * conversion here. The input {@link Message} is an {@link AutoRoutable}, which gets converted to
   * a {@link TypedValue} as the output after slicing the target address and actual payload.
   */
  @Override
  public void route(Message message, Downstream<Message> downstream) {
    final AutoRoutable routable = asAutoRoutable(message);
    final RoutingConfig config = routable.getConfig();
    final TypedValue payload = typedValuePayload(config.getTypeUrl(), routable);
    config
        .getTargetFunctionTypesList()
        .forEach(target -> downstream.forward(sdkFunctionType(target), routable.getId(), payload));
  }

  private static AutoRoutable asAutoRoutable(Message message) {
    try {
      return (AutoRoutable) message;
    } catch (ClassCastException e) {
      throw new RuntimeException(
          "This router only expects messages of type " + AutoRoutable.class.getName(), e);
    }
  }

  private FunctionType sdkFunctionType(TargetFunctionType targetFunctionType) {
    return new FunctionType(targetFunctionType.getNamespace(), targetFunctionType.getType());
  }

  private static TypedValue typedValuePayload(String typeUrl, AutoRoutable routable) {
    return TypedValue.newBuilder()
        .setTypename(typeUrl)
        .setHasValue(true)
        .setValue(routable.getPayloadBytes())
        .addAllMetadata(
            routable.getHeadersList().stream()
                .map(AutoRoutableProtobufRouter::toMetadata)
                .toList())
        .build();
  }

  private static TypedValue.Metadata toMetadata(Header header) {
    return TypedValue.Metadata.newBuilder()
        .setKey(header.getKey())
        .setValue(header.getValue())
        .setHasValue(header.getHasValue())
        .build();
  }
}
