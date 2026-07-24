// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.io.common;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.statefun.flink.io.generated.AutoRoutable;
import org.apache.flink.statefun.flink.io.generated.Header;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.flink.io.generated.TargetFunctionType;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.statefun.sdk.metrics.Metrics;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.junit.jupiter.api.Test;

/**
 * Pins the AutoRoutable → TypedValue conversion contract of {@link AutoRoutableProtobufRouter}:
 * the payload is forwarded to every configured target, and ingested transport headers are carried
 * over as {@code TypedValue.metadata} so they reach remote functions unchanged.
 */
class AutoRoutableProtobufRouterTest {

  private static final String TYPE_URL = "com.googleapis/com.mycomp.foo.OrderMessage";
  private static final TargetFunctionType TARGET_A =
      TargetFunctionType.newBuilder().setNamespace("ns").setType("fn-a").build();
  private static final TargetFunctionType TARGET_B =
      TargetFunctionType.newBuilder().setNamespace("ns").setType("fn-b").build();

  private final AutoRoutableProtobufRouter router = new AutoRoutableProtobufRouter();

  @Test
  void forwardsPayloadToEveryConfiguredTarget() {
    AutoRoutable routable = routableBuilder(TARGET_A, TARGET_B).build();
    CapturingDownstream downstream = new CapturingDownstream();

    router.route(routable, downstream);

    assertThat(downstream.addresses)
        .containsExactly(
            new Address(new org.apache.flink.statefun.sdk.FunctionType("ns", "fn-a"), "id-1"),
            new Address(new org.apache.flink.statefun.sdk.FunctionType("ns", "fn-b"), "id-1"));
    for (Message forwarded : downstream.payloads) {
      TypedValue typedValue = (TypedValue) forwarded;
      assertThat(typedValue.getTypename()).isEqualTo(TYPE_URL);
      assertThat(typedValue.getHasValue()).isTrue();
      assertThat(typedValue.getValue().toStringUtf8()).isEqualTo("payload");
    }
  }

  @Test
  void carriesIngestedHeadersAsTypedValueMetadata() {
    AutoRoutable routable =
        routableBuilder(TARGET_A)
            .addHeaders(header("trace-id", "abc-123"))
            .addHeaders(Header.newBuilder().setKey("null-valued"))
            .addHeaders(header("trace-id", "def-456"))
            .build();
    CapturingDownstream downstream = new CapturingDownstream();

    router.route(routable, downstream);

    TypedValue typedValue = (TypedValue) downstream.payloads.get(0);
    assertThat(typedValue.getMetadataCount()).isEqualTo(3);
    assertThat(typedValue.getMetadata(0).getKey()).isEqualTo("trace-id");
    assertThat(typedValue.getMetadata(0).getHasValue()).isTrue();
    assertThat(typedValue.getMetadata(0).getValue().toStringUtf8()).isEqualTo("abc-123");
    assertThat(typedValue.getMetadata(1).getKey()).isEqualTo("null-valued");
    assertThat(typedValue.getMetadata(1).getHasValue()).isFalse();
    assertThat(typedValue.getMetadata(2).getKey()).isEqualTo("trace-id");
    assertThat(typedValue.getMetadata(2).getHasValue()).isTrue();
    assertThat(typedValue.getMetadata(2).getValue().toStringUtf8()).isEqualTo("def-456");
  }

  @Test
  void routableWithoutHeadersProducesNoMetadata() {
    CapturingDownstream downstream = new CapturingDownstream();

    router.route(routableBuilder(TARGET_A).build(), downstream);

    TypedValue typedValue = (TypedValue) downstream.payloads.get(0);
    assertThat(typedValue.getMetadataCount()).isZero();
  }

  private static AutoRoutable.Builder routableBuilder(TargetFunctionType... targets) {
    RoutingConfig.Builder config = RoutingConfig.newBuilder().setTypeUrl(TYPE_URL);
    for (TargetFunctionType target : targets) {
      config.addTargetFunctionTypes(target);
    }
    return AutoRoutable.newBuilder()
        .setConfig(config)
        .setId("id-1")
        .setPayloadBytes(ByteString.copyFromUtf8("payload"));
  }

  private static Header header(String key, String utf8Value) {
    return Header.newBuilder()
        .setKey(key)
        .setValue(ByteString.copyFromUtf8(utf8Value))
        .setHasValue(true)
        .build();
  }

  private static final class CapturingDownstream implements Router.Downstream<Message> {
    final List<Address> addresses = new ArrayList<>();
    final List<Message> payloads = new ArrayList<>();

    @Override
    public void forward(Address to, Message message) {
      addresses.add(to);
      payloads.add(message);
    }

    @Override
    public Metrics metrics() {
      throw new UnsupportedOperationException("not used in this test");
    }
  }
}
