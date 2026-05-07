// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.flink.statefun.flink.core.generated.Payload;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.junit.jupiter.api.Test;

class PolyglotUtilTest {

  @Test
  void parseProtobufRoundtripsValidWireBytes() {
    Payload original =
        Payload.newBuilder()
            .setClassName("com.foo.Bar")
            .setPayloadBytes(ByteString.copyFromUtf8("ok"))
            .build();

    InputStream wire = new ByteArrayInputStream(original.toByteArray());

    Payload parsed = PolyglotUtil.parseProtobufOrThrow(Payload.parser(), wire);

    assertThat(parsed).isEqualTo(original);
  }

  @Test
  void parseProtobufWrapsIOExceptionInIllegalState() {
    InputStream broken =
        new InputStream() {
          @Override
          public int read() throws IOException {
            throw new IOException("boom");
          }
        };

    assertThatThrownBy(() -> PolyglotUtil.parseProtobufOrThrow(Payload.parser(), broken))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Unable to parse")
        .hasCauseInstanceOf(IOException.class);
  }

  @Test
  void sdkAddressToPolyglotConvertsAllThreeFields() {
    Address sdk = new Address(new FunctionType("ns", "name"), "id-1");

    org.apache.flink.statefun.sdk.reqreply.generated.Address proto =
        PolyglotUtil.sdkAddressToPolyglotAddress(sdk);

    assertThat(proto.getNamespace()).isEqualTo("ns");
    assertThat(proto.getType()).isEqualTo("name");
    assertThat(proto.getId()).isEqualTo("id-1");
  }

  @Test
  void polyglotToSdkAddressIsTheInverseOfSdkToPolyglot() {
    Address sdk = new Address(new FunctionType("ns", "name"), "id-1");

    Address roundtripped =
        PolyglotUtil.polyglotAddressToSdkAddress(PolyglotUtil.sdkAddressToPolyglotAddress(sdk));

    assertThat(roundtripped).isEqualTo(sdk);
  }
}
