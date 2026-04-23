/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.statefun.e2e.k8s;

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

public class RemoteFunctionServer {

  private static final int PORT = 8080;

  public static void main(String[] args) throws IOException {
    StatefulFunctionSpec kafkaCounterSpec =
        StatefulFunctionSpec.builder(KafkaCounterFn.FN_TYPE)
            .withValueSpec(KafkaCounterFn.TOTAL)
            .withSupplier(KafkaCounterFn::new)
            .build();

    StatefulFunctionSpec greeterSpec =
        StatefulFunctionSpec.builder(GreeterFn.FN_TYPE).withSupplier(GreeterFn::new).build();

    StatefulFunctions functions = new StatefulFunctions();
    functions.withStatefulFunction(kafkaCounterSpec);
    functions.withStatefulFunction(greeterSpec);

    RequestReplyHandler handler = functions.requestReplyHandler();

    HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
    server.createContext("/statefun", exchange -> handleRequest(exchange, handler));
    server.start();

    System.out.println("Remote function server started on port " + PORT);
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
      System.out.println("[Server] POST /statefun - " + requestBytes.length + " bytes");
      CompletableFuture<Slice> responseFuture = handler.handle(Slices.wrap(requestBytes));

      responseFuture.whenComplete(
          (result, error) -> {
            if (error != null) {
              System.err.println("[Server] Error handling request: " + error);
              error.printStackTrace(System.err);
              sendResponse(exchange, 500, "Internal Server Error");
            } else {
              byte[] responseBytes = result.toByteArray();
              System.out.println("[Server] Response: " + responseBytes.length + " bytes");
              exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
              try {
                exchange.sendResponseHeaders(200, responseBytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                  os.write(responseBytes);
                }
              } catch (IOException e) {
                System.err.println("[Server] Error sending response: " + e.getMessage());
              }
            }
          });
    } catch (IOException e) {
      System.err.println("[Server] Error reading request: " + e.getMessage());
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
      System.err.println("Error sending error response: " + e.getMessage());
    }
  }
}
