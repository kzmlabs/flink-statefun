// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

/**
 * Pins the reflective contract that {@link FlinkConfigExtractor#reflectivelyExtractFromEnv} relies
 * on: {@code StreamExecutionEnvironment#getConfiguration} exists, is reflectively accessible, and
 * returns the same Configuration the SDK feeds back into binders / Modules.
 *
 * <p>The class uses reflection because {@code getConfiguration()} is package-private upstream.
 * If Flink ever renames or removes that method, this test fails immediately rather than silently
 * skipping StateFun config extraction at job-graph build time.
 */
class FlinkConfigExtractorTest {

  @Test
  void extractsConfigurationFromLocalStreamExecutionEnvironment() {
    Configuration sentinel = new Configuration();
    sentinel.setString("statefun.test.sentinel", "match-me");

    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironment(1, sentinel);

    Configuration extracted = FlinkConfigExtractor.reflectivelyExtractFromEnv(env);

    assertThat(extracted).isNotNull();
    // The extracted config must reflect the values we supplied to createLocalEnvironment —
    // proves the reflection lands on the right field, not a fresh empty Configuration.
    assertThat(extracted.getString("statefun.test.sentinel", null)).isEqualTo("match-me");
  }

  @Test
  void extractedConfigIsReadableForFurtherStateFunsLookups() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

    Configuration extracted = FlinkConfigExtractor.reflectivelyExtractFromEnv(env);

    // Empty config is fine — the API contract is "return a Configuration, never throw on
    // missing keys". Pin that callers can read defaults safely.
    assertThat(extracted.getString("any.missing.key", "fallback")).isEqualTo("fallback");
  }
}
