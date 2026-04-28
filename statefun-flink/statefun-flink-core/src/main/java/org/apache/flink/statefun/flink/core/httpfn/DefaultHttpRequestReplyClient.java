// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.httpfn;

import static org.apache.flink.statefun.flink.core.common.PolyglotUtil.parseProtobufOrThrow;
import static org.apache.flink.util.Preconditions.checkState;

import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.ToFunctionRequestSummary;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.apache.flink.util.IOUtils;

final class DefaultHttpRequestReplyClient implements RequestReplyClient {
  private static final MediaType MEDIA_TYPE_BINARY = MediaType.parse("application/octet-stream");

  private final HttpUrl url;
  private final OkHttpClient client;
  private final BooleanSupplier isShutdown;

  DefaultHttpRequestReplyClient(HttpUrl url, OkHttpClient client, BooleanSupplier isShutdown) {
    this.url = Objects.requireNonNull(url);
    this.client = Objects.requireNonNull(client);
    this.isShutdown = Objects.requireNonNull(isShutdown);
  }

  @Override
  public CompletableFuture<FromFunction> call(
      ToFunctionRequestSummary requestSummary,
      RemoteInvocationMetrics metrics,
      ToFunction toFunction) {
    Request request =
        new Request.Builder()
            .url(url)
            .post(RequestBody.create(MEDIA_TYPE_BINARY, toFunction.toByteArray()))
            .build();

    Call newCall = client.newCall(request);
    RetryingCallback callback =
        new RetryingCallback(requestSummary, metrics, newCall.timeout(), isShutdown);
    callback.attachToCall(newCall);
    return callback.future().thenApply(DefaultHttpRequestReplyClient::parseResponse);
  }

  private static FromFunction parseResponse(Response response) {
    final InputStream httpResponseBody = responseBody(response);
    try {
      return parseProtobufOrThrow(FromFunction.parser(), httpResponseBody);
    } finally {
      IOUtils.closeQuietly(httpResponseBody);
    }
  }

  private static InputStream responseBody(Response httpResponse) {
    checkState(httpResponse.isSuccessful(), "Unexpected HTTP status code %s", httpResponse.code());
    checkState(httpResponse.body() != null, "Unexpected empty HTTP response (no body)");
    checkState(
        Objects.equals(httpResponse.body().contentType(), MEDIA_TYPE_BINARY),
        "Wrong HTTP content-type %s",
        httpResponse.body().contentType());
    return httpResponse.body().byteStream();
  }
}
