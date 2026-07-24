// Copyright 2026 Kzmlabs
// SPDX-License-Identifier: Apache-2.0

package io.github.kzmlabs.quickstart;

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

/** Extracts a name from a JSON payload and emits a greeting to the Kafka egress. */
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
    String greeting = "{\"greeting\":\"Hello, " + jsonEscape(name) + "!\"}";

    context.send(
        KafkaEgressMessage.forEgress(EGRESS_ID)
            .withTopic(RESULTS_TOPIC)
            .withUtf8Key(context.self().id())
            .withUtf8Value(greeting)
            .withUtf8Header("greeted-by", "quickstart-greeter") // Kafka record header, since KZM-3.4
            .build());

    return context.done();
  }

  /** Minimal JSON string escape so the emitted greeting stays valid for names with quotes/backslashes. */
  private static String jsonEscape(String s) {
    StringBuilder sb = new StringBuilder(s.length() + 8);
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"':
          sb.append("\\\"");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        default:
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
      }
    }
    return sb.toString();
  }
}
