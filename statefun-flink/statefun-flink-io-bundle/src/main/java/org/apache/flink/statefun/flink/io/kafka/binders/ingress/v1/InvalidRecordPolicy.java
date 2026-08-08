// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.io.kafka.binders.ingress.v1;

import java.io.Serializable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

/**
 * Effective invalid-record policy of one ingress topic: what to do with an invalid record (skip it
 * or fail the job) and, for skip, at which level to log it. Serializable because it ships to the
 * cluster inside the ingress deserializer.
 */
final class InvalidRecordPolicy implements Serializable {

  private static final long serialVersionUID = 1L;

  enum Action {
    SKIP,
    FAIL
  }

  enum LogLevel {
    DEBUG,
    INFO,
    WARN,
    ERROR
  }

  private static final InvalidRecordPolicy DEFAULT = new InvalidRecordPolicy(Action.SKIP, LogLevel.WARN);

  private final Action action;
  private final LogLevel logLevel;

  private InvalidRecordPolicy(Action action, LogLevel logLevel) {
    this.action = action;
    this.logLevel = logLevel;
  }

  /** The out-of-the-box policy when no yaml is given: skip with a WARN log per record. */
  static InvalidRecordPolicy defaults() {
    return DEFAULT;
  }

  static InvalidRecordPolicy skip(LogLevel logLevel) {
    return new InvalidRecordPolicy(Action.SKIP, logLevel);
  }

  static InvalidRecordPolicy fail() {
    return new InvalidRecordPolicy(Action.FAIL, LogLevel.ERROR);
  }

  /**
   * Parses an invalidRecordHandling spec node: type skip or fail, optional logLevel (debug, info,
   * warn or error) applicable to skip only. Unknown values fail spec parsing with the valid values
   * listed.
   */
  static InvalidRecordPolicy fromSpecNode(JsonNode node) {
    JsonNode typeNode = node.get("type");
    String type = typeNode == null || typeNode.isNull() ? null : typeNode.asText();
    if ("skip".equals(type)) {
      return skip(parseLogLevel(node));
    }
    if ("fail".equals(type)) {
      if (node.has("logLevel")) {
        throw new IllegalArgumentException("invalidRecordHandling: logLevel is only applicable to type: skip");
      }
      return fail();
    }
    throw new IllegalArgumentException("Invalid invalidRecordHandling type: " + type + "; valid values are [skip, fail]");
  }

  private static LogLevel parseLogLevel(JsonNode node) {
    JsonNode levelNode = node.get("logLevel");
    if (levelNode == null || levelNode.isNull()) {
      return LogLevel.WARN;
    }
    try {
      return LogLevel.valueOf(levelNode.asText().toUpperCase(java.util.Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid invalidRecordHandling logLevel: " + levelNode.asText() + "; valid values are [debug, info, warn, error]", e);
    }
  }

  Action action() {
    return action;
  }

  LogLevel logLevel() {
    return logLevel;
  }

  /** Resolves this policy to the handler strategy the deserializer applies to invalid records. */
  InvalidRecordHandler handler() {
    return action == Action.FAIL ? new FailInvalidRecordHandler() : new SkipInvalidRecordHandler(logLevel);
  }
}
