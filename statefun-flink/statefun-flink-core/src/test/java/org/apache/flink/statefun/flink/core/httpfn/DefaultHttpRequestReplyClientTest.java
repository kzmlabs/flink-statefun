// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.httpfn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.reqreply.ToFunctionRequestSummary;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.IncompleteInvocationContext;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.InvocationResponse;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end test for {@link DefaultHttpRequestReplyClient} against a real {@link MockWebServer}.
 * Pins the wire format (binary protobuf POST, application/octet-stream content-type) and the
 * response content-type validation.
 */
class DefaultHttpRequestReplyClientTest {

  private static final ToFunctionRequestSummary SUMMARY =
      new ToFunctionRequestSummary(new Address(new FunctionType("ns", "fn"), "id"), 0, 0, 0);

  private MockWebServer server;
  private OkHttpClient httpClient;
  private CountingMetrics metrics;

  @BeforeEach
  void setUp() throws IOException {
    server = new MockWebServer();
    server.start();
    httpClient = new OkHttpClient();
    metrics = new CountingMetrics();
  }

  @AfterEach
  void tearDown() throws IOException {
    server.shutdown();
  }

  @Test
  void postsToFunctionAsBinaryAndDecodesFromFunctionResponse() throws Exception {
    FromFunction expected =
        FromFunction.newBuilder()
            .setInvocationResult(InvocationResponse.newBuilder().build())
            .build();
    server.enqueue(
        new MockResponse()
            .setHeader("Content-Type", "application/octet-stream")
            .setBody(new Buffer().write(expected.toByteArray())));

    DefaultHttpRequestReplyClient client = newClient();

    FromFunction actual =
        client.call(SUMMARY, metrics, ToFunction.getDefaultInstance()).get(10, TimeUnit.SECONDS);

    assertThat(actual).isEqualTo(expected);
    RecordedRequest request = server.takeRequest(5, TimeUnit.SECONDS);
    assertThat(request).isNotNull();
    assertThat(request.getMethod()).isEqualTo("POST");
    assertThat(request.getHeader("Content-Type"))
        .isNotNull()
        .startsWith("application/octet-stream");
  }

  @Test
  void requestBodyIsTheSerializedToFunctionPayload() throws Exception {
    server.enqueue(
        new MockResponse()
            .setHeader("Content-Type", "application/octet-stream")
            .setBody(new Buffer().write(FromFunction.getDefaultInstance().toByteArray())));

    DefaultHttpRequestReplyClient client = newClient();

    ToFunction payload =
        ToFunction.newBuilder()
            .setInvocation(
                ToFunction.InvocationBatchRequest.newBuilder()
                    .addInvocations(
                        ToFunction.Invocation.newBuilder()
                            .setArgument(
                                org.apache.flink.statefun.sdk.reqreply.generated.TypedValue
                                    .newBuilder()
                                    .setValue(ByteString.copyFromUtf8("hello"))
                                    .build())))
            .build();

    client.call(SUMMARY, metrics, payload).get(10, TimeUnit.SECONDS);

    RecordedRequest request = server.takeRequest(5, TimeUnit.SECONDS);
    assertThat(request.getBodySize()).isEqualTo(payload.getSerializedSize());
    ToFunction sentBack = ToFunction.parseFrom(request.getBody().readByteArray());
    assertThat(sentBack).isEqualTo(payload);
  }

  @Test
  void incompleteInvocationContextResponseIsDecodedTransparently() throws Exception {
    FromFunction expected =
        FromFunction.newBuilder()
            .setIncompleteInvocationContext(IncompleteInvocationContext.newBuilder().build())
            .build();
    server.enqueue(
        new MockResponse()
            .setHeader("Content-Type", "application/octet-stream")
            .setBody(new Buffer().write(expected.toByteArray())));

    DefaultHttpRequestReplyClient client = newClient();

    FromFunction actual =
        client.call(SUMMARY, metrics, ToFunction.getDefaultInstance()).get(10, TimeUnit.SECONDS);

    assertThat(actual.hasIncompleteInvocationContext()).isTrue();
  }

  @Test
  void wrongContentTypeIsRejectedWithIllegalState() {
    // Pin: the response Content-Type guard rejects responses that are not octet-stream.
    server.enqueue(
        new MockResponse()
            .setHeader("Content-Type", "application/json")
            .setBody("{\"foo\": \"bar\"}"));

    DefaultHttpRequestReplyClient client = newClient();

    assertThatThrownBy(
            () ->
                client
                    .call(SUMMARY, metrics, ToFunction.getDefaultInstance())
                    .get(10, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(IllegalStateException.class)
        .satisfies(t -> assertThat(t.getCause()).hasMessageContaining("Wrong HTTP content-type"));
  }

  @Test
  void rejectsNullUrl() {
    assertThatThrownBy(() -> new DefaultHttpRequestReplyClient(null, httpClient, () -> false))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void rejectsNullHttpClient() {
    assertThatThrownBy(
            () ->
                new DefaultHttpRequestReplyClient(server.url("/").newBuilder().build(), null, () -> false))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void rejectsNullIsShutdown() {
    assertThatThrownBy(
            () ->
                new DefaultHttpRequestReplyClient(
                    server.url("/").newBuilder().build(), httpClient, null))
        .isInstanceOf(NullPointerException.class);
  }

  private DefaultHttpRequestReplyClient newClient() {
    HttpUrl url = server.url("/").newBuilder().build();
    return new DefaultHttpRequestReplyClient(url, httpClient, () -> false);
  }

  private static final class CountingMetrics implements RemoteInvocationMetrics {
    @Override
    public void remoteInvocationFailures() {}

    @Override
    public void remoteInvocationLatency(long elapsed) {}
  }
}
