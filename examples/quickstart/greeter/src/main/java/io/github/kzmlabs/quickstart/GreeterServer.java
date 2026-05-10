// Copyright 2026 Kzmlabs
// SPDX-License-Identifier: Apache-2.0

package io.github.kzmlabs.quickstart;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.Slices;

/** Minimal HTTP server exposing the GreeterFn at POST /statefun. */
public final class GreeterServer {

  private static final int PORT = 8080;

  public static void main(String[] args) throws IOException {
    StatefulFunctionSpec greeterSpec =
        StatefulFunctionSpec.builder(GreeterFn.FN_TYPE).withSupplier(GreeterFn::new).build();

    StatefulFunctions functions = new StatefulFunctions();
    functions.withStatefulFunction(greeterSpec);

    RequestReplyHandler handler = functions.requestReplyHandler();

    HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
    server.createContext("/statefun", exchange -> handleRequest(exchange, handler));
    server.start();

    System.out.println("Greeter server started on port " + PORT);
  }

  private static void handleRequest(HttpExchange exchange, RequestReplyHandler handler) {
    if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      sendResponse(exchange, 200, "OK");
      return;
    }
    if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
      sendResponse(exchange, 405, "Method Not Allowed");
      return;
    }

    try {
      byte[] requestBytes = exchange.getRequestBody().readAllBytes();
      CompletableFuture<Slice> responseFuture = handler.handle(Slices.wrap(requestBytes));

      responseFuture.whenComplete(
          (result, error) -> {
            if (error != null) {
              System.err.println("[Greeter] Error handling request: " + error);
              error.printStackTrace(System.err);
              sendResponse(exchange, 500, "Internal Server Error");
            } else {
              byte[] responseBytes = result.toByteArray();
              exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
              try {
                exchange.sendResponseHeaders(200, responseBytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                  os.write(responseBytes);
                }
              } catch (IOException e) {
                System.err.println("[Greeter] Error sending response: " + e.getMessage());
              }
            }
          });
    } catch (IOException e) {
      System.err.println("[Greeter] Error reading request: " + e.getMessage());
      sendResponse(exchange, 500, "Internal Server Error");
    }
  }

  private static void sendResponse(HttpExchange exchange, int statusCode, String body) {
    try {
      byte[] bytes = body.getBytes();
      exchange.sendResponseHeaders(statusCode, bytes.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(bytes);
      }
    } catch (IOException e) {
      System.err.println("[Greeter] Error sending error response: " + e.getMessage());
    }
  }
}
