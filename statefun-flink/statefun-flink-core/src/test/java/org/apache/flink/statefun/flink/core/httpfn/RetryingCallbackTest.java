// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.httpfn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.Timeout;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.reqreply.ToFunctionRequestSummary;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Drives the {@link RetryingCallback} retry state machine against a real {@link MockWebServer} so
 * the success / retryable / non-retryable / shutdown / timeout paths all execute end-to-end. Uses
 * a deterministic in-process HTTP server — no Docker, no external service.
 */
class RetryingCallbackTest {

  private static final ToFunctionRequestSummary SUMMARY =
      new ToFunctionRequestSummary(
          new Address(new FunctionType("ns", "fn"), "id"), 0, 0, 0);

  private MockWebServer server;
  private OkHttpClient client;
  private CountingMetrics metrics;

  @BeforeEach
  void setUp() throws IOException {
    server = new MockWebServer();
    server.start();
    client = new OkHttpClient();
    metrics = new CountingMetrics();
  }

  @AfterEach
  void tearDown() throws IOException {
    server.shutdown();
  }

  @Test
  void successfulResponseCompletesFutureAndRecordsLatency() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(200).setBody("ok"));

    Response response = invokeAndAwait(callback(neverShutdown()));

    assertThat(response.isSuccessful()).isTrue();
    assertThat(response.body().string()).isEqualTo("ok");
    assertThat(metrics.latencyRecorded.get()).isEqualTo(1);
    assertThat(metrics.failuresRecorded.get()).isZero();
  }

  @Test
  void retryableServerErrorRetriesUntilSuccess() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(500));
    server.enqueue(new MockResponse().setResponseCode(500));
    server.enqueue(new MockResponse().setResponseCode(200).setBody("after-retries"));

    Response response = invokeAndAwait(callback(neverShutdown()));

    assertThat(response.isSuccessful()).isTrue();
    assertThat(response.body().string()).isEqualTo("after-retries");
    // Three call attempts -> three latency records (one per attempt).
    assertThat(metrics.latencyRecorded.get()).isEqualTo(3);
  }

  @Test
  void retryableHttp429IsRetried() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(429));
    server.enqueue(new MockResponse().setResponseCode(200).setBody("ok"));

    Response response = invokeAndAwait(callback(neverShutdown()));

    assertThat(response.isSuccessful()).isTrue();
    assertThat(response.body().string()).isEqualTo("ok");
  }

  @Test
  void retryableHttp408IsRetried() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(408));
    server.enqueue(new MockResponse().setResponseCode(200).setBody("ok"));

    Response response = invokeAndAwait(callback(neverShutdown()));

    assertThat(response.isSuccessful()).isTrue();
  }

  @Test
  void nonRetryableClientErrorFailsFutureWithIllegalState() {
    server.enqueue(new MockResponse().setResponseCode(404));

    RetryingCallback cb = callback(neverShutdown());
    enqueueRequest(cb);

    assertThatThrownBy(() -> cb.future().get(5, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(IllegalStateException.class)
        .satisfies(
            t ->
                assertThat(t.getCause())
                    .hasMessageContaining("Non successful HTTP response code 404"));
  }

  @Test
  void networkFailureWithBackoffExhaustedFailsFuture() {
    // Server closed before request -> connection refused -> IOException -> onFailure.
    // Use a backoff timeout of 1ns so the first retry attempt blows past the deadline.
    int unboundPort = server.getPort();
    try {
      server.shutdown();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    RetryingCallback cb =
        new RetryingCallback(SUMMARY, metrics, fixedTimeoutNanos(1L), neverShutdown());
    Request request =
        new Request.Builder().url("http://127.0.0.1:" + unboundPort + "/").build();
    cb.attachToCall(client.newCall(request));

    assertThatThrownBy(() -> cb.future().get(10, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(IllegalStateException.class)
        .satisfies(
            t ->
                assertThat(t.getCause()).hasMessageContaining("Maximal request time has elapsed"));
    assertThat(metrics.failuresRecorded.get()).isPositive();
  }

  @Test
  void shutdownDuringRequestSurfacesAsIllegalState() throws IOException {
    int unboundPort = server.getPort();
    server.shutdown();

    AtomicInteger shutdownFlag = new AtomicInteger(1); // already "shut down" before first call
    RetryingCallback cb =
        new RetryingCallback(SUMMARY, metrics, fixedTimeoutNanos(Long.MAX_VALUE), () -> shutdownFlag.get() != 0);
    Request request =
        new Request.Builder().url("http://127.0.0.1:" + unboundPort + "/").build();
    cb.attachToCall(client.newCall(request));

    assertThatThrownBy(() -> cb.future().get(10, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(IllegalStateException.class)
        .satisfies(t -> assertThat(t.getCause()).hasMessageContaining("during shutdown"));
  }

  @Test
  void retryableHttp500WithExhaustedBackoffFailsWithDescriptiveMessage() {
    // Always 500; backoff timeout is 1ns so the first retry attempt fails the deadline.
    server.enqueue(new MockResponse().setResponseCode(500));

    RetryingCallback cb =
        new RetryingCallback(SUMMARY, metrics, fixedTimeoutNanos(1L), neverShutdown());
    enqueueRequest(cb);

    assertThatThrownBy(() -> cb.future().get(10, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(IllegalStateException.class)
        .satisfies(
            t -> assertThat(t.getCause()).hasMessageContaining("invalid HTTP response code 500"));
  }

  @Test
  void rejectsNullIsShutdownSupplier() {
    assertThatThrownBy(
            () ->
                new RetryingCallback(
                    SUMMARY, metrics, fixedTimeoutNanos(Long.MAX_VALUE), null))
        .isInstanceOf(NullPointerException.class);
  }

  // --------------------- helpers ---------------------

  private Response invokeAndAwait(RetryingCallback cb) throws Exception {
    enqueueRequest(cb);
    return cb.future().get(10, TimeUnit.SECONDS);
  }

  private void enqueueRequest(RetryingCallback cb) {
    Request request = new Request.Builder().url(server.url("/").toString()).build();
    cb.attachToCall(client.newCall(request));
  }

  private RetryingCallback callback(java.util.function.BooleanSupplier isShutdown) {
    // 5-second timeout window — enough room for a few retries of 10ms+ backoff.
    return new RetryingCallback(
        SUMMARY, metrics, fixedTimeoutNanos(TimeUnit.SECONDS.toNanos(5)), isShutdown);
  }

  private static java.util.function.BooleanSupplier neverShutdown() {
    return () -> false;
  }

  /**
   * Returns an okio {@link Timeout} whose {@code timeoutNanos()} returns the given value.
   * RetryingCallback only reads {@code timeoutNanos()} from the timeout — anything else can be
   * default.
   */
  private static Timeout fixedTimeoutNanos(long nanos) {
    return new Timeout().timeout(nanos, TimeUnit.NANOSECONDS);
  }

  private static final class CountingMetrics implements RemoteInvocationMetrics {
    final AtomicInteger latencyRecorded = new AtomicInteger();
    final AtomicInteger failuresRecorded = new AtomicInteger();

    @Override
    public void remoteInvocationFailures() {
      failuresRecorded.incrementAndGet();
    }

    @Override
    public void remoteInvocationLatency(long elapsed) {
      latencyRecorded.incrementAndGet();
    }
  }

}
