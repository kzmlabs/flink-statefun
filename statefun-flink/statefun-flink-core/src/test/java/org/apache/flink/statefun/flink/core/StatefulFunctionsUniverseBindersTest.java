// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.statefun.flink.core.message.MessageFactoryKey;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.flink.io.spi.SinkProvider;
import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.FunctionTypeNamespaceMatcher;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.io.Router;
import org.junit.jupiter.api.Test;

class StatefulFunctionsUniverseBindersTest {

  @Test
  void bindIngressRecordsSpecKeyedById() {
    StatefulFunctionsUniverse u = newUniverse();
    IngressIdentifier<String> id = new IngressIdentifier<>(String.class, "ns", "in");
    IngressSpec<String> spec = ingressSpec(id);

    u.bindIngress(spec);

    assertThat(u.ingress()).containsEntry(id, spec);
  }

  @Test
  void bindIngressRejectsDuplicateId() {
    StatefulFunctionsUniverse u = newUniverse();
    IngressIdentifier<String> id = new IngressIdentifier<>(String.class, "ns", "in");
    u.bindIngress(ingressSpec(id));

    assertThatThrownBy(() -> u.bindIngress(ingressSpec(id)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("previously defined");
  }

  @Test
  void bindEgressRecordsSpecKeyedById() {
    StatefulFunctionsUniverse u = newUniverse();
    EgressIdentifier<String> id = new EgressIdentifier<>("ns", "out", String.class);
    EgressSpec<String> spec = egressSpec(id);

    u.bindEgress(spec);

    assertThat(u.egress()).containsEntry(id, spec);
  }

  @Test
  void bindRouterAccumulatesPerIngress() {
    StatefulFunctionsUniverse u = newUniverse();
    IngressIdentifier<String> id = new IngressIdentifier<>(String.class, "ns", "in");
    Router<String> r1 = (msg, sink) -> {};
    Router<String> r2 = (msg, sink) -> {};

    u.bindIngressRouter(id, r1);
    u.bindIngressRouter(id, r2);

    assertThat(u.routers().get(id)).containsExactly(r1, r2);
  }

  @Test
  void bindFunctionProviderByTypeRejectsDuplicate() {
    StatefulFunctionsUniverse u = newUniverse();
    FunctionType ft = new FunctionType("ns", "fn");
    StatefulFunctionProvider provider = ignored -> null;

    u.bindFunctionProvider(ft, provider);

    assertThatThrownBy(() -> u.bindFunctionProvider(ft, provider))
        .isInstanceOf(IllegalStateException.class);
    assertThat(u.functions()).containsEntry(ft, provider);
  }

  @Test
  void bindFunctionProviderByNamespaceMatcherRecordsByNamespace() {
    StatefulFunctionsUniverse u = newUniverse();
    StatefulFunctionProvider provider = ignored -> null;

    u.bindFunctionProvider(FunctionTypeNamespaceMatcher.targetNamespace("ns"), provider);

    assertThat(u.namespaceFunctions()).containsEntry("ns", provider);
  }

  @Test
  void bindSourceProviderRejectsDuplicate() {
    StatefulFunctionsUniverse u = newUniverse();
    IngressType type = new IngressType("ns", "kafka");
    SourceProvider source =
        new SourceProvider() {
          @Override
          public <T, SplitT extends org.apache.flink.api.connector.source.SourceSplit, EnumChckT>
              org.apache.flink.api.connector.source.Source<T, SplitT, EnumChckT> forSpec(
                  org.apache.flink.statefun.sdk.io.IngressSpec<T> spec) {
            return null;
          }
        };

    u.bindSourceProvider(type, source);

    assertThatThrownBy(() -> u.bindSourceProvider(type, source))
        .isInstanceOf(IllegalStateException.class);
    assertThat(u.sources()).containsEntry(type, source);
  }

  @Test
  void bindSinkProviderRecordsBinding() {
    StatefulFunctionsUniverse u = newUniverse();
    EgressType type = new EgressType("ns", "kafka");
    SinkProvider sink =
        new SinkProvider() {
          @Override
          public <T> org.apache.flink.api.connector.sink2.Sink<T> forSpec(EgressSpec<T> spec) {
            return null;
          }
        };

    u.bindSinkProvider(type, sink);

    assertThat(u.sinks()).containsEntry(type, sink);
  }

  @Test
  void universeExposesMessageFactoryKey() {
    MessageFactoryKey key =
        MessageFactoryKey.forType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, null);
    StatefulFunctionsUniverse u = new StatefulFunctionsUniverse(key);

    assertThat(u.messageFactoryKey()).isSameAs(key);
    assertThat(u.types()).isNotNull();
  }

  @Test
  void resolveExtensionThrowsForMissingTypeName() {
    StatefulFunctionsUniverse u = newUniverse();

    assertThatThrownBy(
            () ->
                u.resolveExtension(
                    org.apache.flink.statefun.sdk.TypeName.parseFrom("a/b"), Object.class))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("does not exist");
  }

  private static StatefulFunctionsUniverse newUniverse() {
    return new StatefulFunctionsUniverse(
        MessageFactoryKey.forType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, null));
  }

  private static <T> IngressSpec<T> ingressSpec(IngressIdentifier<T> id) {
    return new IngressSpec<T>() {
      @Override
      public IngressIdentifier<T> id() {
        return id;
      }

      @Override
      public IngressType type() {
        return new IngressType("ns", "test");
      }
    };
  }

  private static <T> EgressSpec<T> egressSpec(EgressIdentifier<T> id) {
    return new EgressSpec<T>() {
      @Override
      public EgressIdentifier<T> id() {
        return id;
      }

      @Override
      public EgressType type() {
        return new EgressType("ns", "test");
      }
    };
  }

}
