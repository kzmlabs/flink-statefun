// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.httpfn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.core.reqreply.ClassLoaderSafeRequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultHttpRequestReplyClientFactoryTest {

  private final DefaultHttpRequestReplyClientFactory factory =
      DefaultHttpRequestReplyClientFactory.INSTANCE;

  @BeforeEach
  void resetSingleton() {
    factory.cleanup();
  }

  @AfterEach
  void cleanup() {
    factory.cleanup();
  }

  @Test
  void createTransportClientReturnsAClient() throws URISyntaxException {
    RequestReplyClient client =
        factory.createTransportClient(emptyTransportSpec(), new URI("http://localhost:8080"));

    assertThat(client).isNotNull();
  }

  /**
   * The factory wraps with {@link ClassLoaderSafeRequestReplyClient} when the calling thread's
   * context classloader differs from the factory's own — the runtime scenario where user-provided
   * modules drive transport creation through a different classloader.
   */
  @Test
  void createsClassLoaderSafeWrapperWhenContextClassLoaderDiffersFromFactoryClassLoader() {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    ClassLoader different = new ClassLoader(null) {};
    Thread.currentThread().setContextClassLoader(different);
    try {
      RequestReplyClient client =
          factory.createTransportClient(emptyTransportSpec(), uri("http://localhost:8080"));

      assertThat(client).isInstanceOf(ClassLoaderSafeRequestReplyClient.class);
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Test
  void createsRawClientWhenContextClassLoaderMatchesFactoryClassLoader() {
    // When TCCL == factory's CL, no wrapper is needed. Pin that the unwrapped client surfaces.
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(factory.getClass().getClassLoader());
    try {
      RequestReplyClient client =
          factory.createTransportClient(emptyTransportSpec(), uri("http://localhost:8080"));

      assertThat(client).isNotInstanceOf(ClassLoaderSafeRequestReplyClient.class);
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Test
  void invalidTransportPropertiesFailLoudly() {
    // Pin: malformed transport-properties JSON wraps as a RuntimeException so misuse is loud
    // rather than producing a misconfigured client at runtime.
    ObjectNode bad = new ObjectMapper().createObjectNode();
    bad.put("timeouts", "not-an-object-or-spec"); // Wrong shape: timeouts must be a sub-object.

    assertThatThrownBy(() -> factory.createTransportClient(bad, uri("http://localhost:8080")))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Unable to parse transport client properties");
  }

  @Test
  void cleanupIsIdempotent() {
    // Ensure the factory's shared OkHttpClient is initialized.
    factory.createTransportClient(emptyTransportSpec(), uri("http://localhost:8080"));

    factory.cleanup();
    // Second cleanup must not blow up — pin idempotency since the factory is a JVM-singleton
    // and tests share its state.
    factory.cleanup();
  }

  @Test
  void cleanupBeforeAnyClientCreatedIsNoOp() {
    // Pin: cleanup() on a fresh factory (no shared client yet) must be safe.
    factory.cleanup();
  }

  private static ObjectNode emptyTransportSpec() {
    return new ObjectMapper().createObjectNode();
  }

  private static URI uri(String s) {
    try {
      return new URI(s);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
