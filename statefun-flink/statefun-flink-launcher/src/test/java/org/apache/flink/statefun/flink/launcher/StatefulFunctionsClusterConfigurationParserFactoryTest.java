// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.launcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.junit.jupiter.api.Test;

class StatefulFunctionsClusterConfigurationParserFactoryTest {

  @Test
  void parsesConfigDirAndDefaultsForOtherOptions() throws Exception {
    StatefulFunctionsClusterConfiguration cfg = parse("--configDir", "/etc/flink/conf");

    assertThat(cfg.getConfigDir()).isEqualTo("/etc/flink/conf");
    assertThat(cfg.getJobId()).isNull();
    // Default parallelism sentinel — entrypoint will fall back to cluster default.
    assertThat(cfg.getParallelism()).isEqualTo(ExecutionConfig.PARALLELISM_DEFAULT);
    assertThat(cfg.getDynamicProperties()).isEmpty();
    assertThat(cfg.getSavepointRestoreSettings())
        .isEqualTo(SavepointRestoreSettings.none());
  }

  @Test
  void parsesJobIdFromHexString() throws Exception {
    JobID jobId = JobID.generate();
    StatefulFunctionsClusterConfiguration cfg =
        parse("--configDir", "/etc/flink/conf", "--job-id", jobId.toHexString());

    assertThat(cfg.getJobId()).isEqualTo(jobId);
  }

  @Test
  void rejectsMalformedJobId() {
    assertThatThrownBy(() -> parse("--configDir", "/c", "--job-id", "not-a-hex-id"))
        .isInstanceOf(FlinkParseException.class)
        .hasMessageContaining("--job-id");
  }

  @Test
  void parsesPositiveParallelism() throws Exception {
    StatefulFunctionsClusterConfiguration cfg = parse("--configDir", "/c", "-p", "4");

    assertThat(cfg.getParallelism()).isEqualTo(4);
  }

  @Test
  void rejectsNonNumericParallelism() {
    assertThatThrownBy(() -> parse("--configDir", "/c", "-p", "many"))
        .isInstanceOf(FlinkParseException.class)
        .hasMessageContaining("--parallelism");
  }

  @Test
  void rejectsZeroOrNegativeParallelism() {
    // The current parser wraps NumberFormatException into FlinkParseException but lets
    // the explicit "must be at least 1" IllegalArgumentException bubble up directly.
    // TODO(parser): wrap this IAE in FlinkParseException for symmetric error handling.
    assertThatThrownBy(() -> parse("--configDir", "/c", "-p", "0"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Parallelism must be at least 1");

    assertThatThrownBy(() -> parse("--configDir", "/c", "-p", "-3"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void parsesSavepointPath() throws Exception {
    StatefulFunctionsClusterConfiguration cfg = parse("--configDir", "/c", "-s", "s3://savepoints/x");

    SavepointRestoreSettings settings = cfg.getSavepointRestoreSettings();
    assertThat(settings.getRestorePath()).isEqualTo("s3://savepoints/x");
    assertThat(settings.allowNonRestoredState()).isFalse();
  }

  @Test
  void parsesSavepointAllowNonRestored() throws Exception {
    StatefulFunctionsClusterConfiguration cfg =
        parse("--configDir", "/c", "-s", "s3://sp", "-n");

    assertThat(cfg.getSavepointRestoreSettings().allowNonRestoredState()).isTrue();
  }

  @Test
  void capturesDynamicPropertiesAndPositionalArgs() throws Exception {
    StatefulFunctionsClusterConfiguration cfg =
        parse(
            "--configDir",
            "/c",
            "-Dkey1=val1",
            "-Dkey2=val2",
            "positional-1",
            "positional-2");

    assertThat(cfg.getDynamicProperties())
        .containsEntry("key1", "val1")
        .containsEntry("key2", "val2");
    assertThat(cfg.getArgs()).containsExactly("positional-1", "positional-2");
  }

  private static StatefulFunctionsClusterConfiguration parse(String... args) throws Exception {
    CommandLineParser<StatefulFunctionsClusterConfiguration> parser =
        new CommandLineParser<>(new StatefulFunctionsClusterConfigurationParserFactory());
    return parser.parse(args);
  }
}
