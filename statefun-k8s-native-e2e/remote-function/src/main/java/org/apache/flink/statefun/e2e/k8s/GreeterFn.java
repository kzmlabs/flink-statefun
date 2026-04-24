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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

/** Extracts a name from a JSON payload and emits a greeting to Kafka. */
public final class GreeterFn implements StatefulFunction {

  static final TypeName FN_TYPE = TypeName.typeNameOf("greeter", "fn");
  static final TypeName EGRESS_ID = TypeName.typeNameOf("greeter", "results");
  static final String RESULTS_TOPIC = "greeter.results";

  static final TypeName JSON_STRING_TYPE_NAME = TypeName.typeNameOf("greeter", "json-string");

  static final Type<String> JSON_STRING_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          JSON_STRING_TYPE_NAME,
          str -> str.getBytes(StandardCharsets.UTF_8),
          bytes -> new String(bytes, StandardCharsets.UTF_8));

  private static final Pattern NAME_PATTERN = Pattern.compile("\"name\"\\s*:\\s*\"([^\"]+)\"");

  @Override
  public CompletableFuture<Void> apply(Context context, Message message) {
    if (!message.is(JSON_STRING_TYPE)) {
      return context.done();
    }

    String input = message.as(JSON_STRING_TYPE);
    Matcher m = NAME_PATTERN.matcher(input);
    String name = m.find() ? m.group(1) : input;
    String greeting = "{\"greeting\":\"Hello, " + name + "!\"}";

    context.send(
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic(RESULTS_TOPIC)
            .withUtf8Key(context.self().id())
            .withUtf8Value(greeting)
            .build());

    return context.done();
  }
}
