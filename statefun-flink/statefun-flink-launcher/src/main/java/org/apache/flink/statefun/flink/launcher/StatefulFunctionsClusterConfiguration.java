// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.launcher;

import static java.util.Objects.requireNonNull;

import java.util.Properties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.entrypoint.EntrypointClusterConfiguration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

/**
 * Configuration for the {@link StatefulFunctionsClusterEntryPoint}.
 *
 * <p>This class was copied from Apache Flink.
 */
final class StatefulFunctionsClusterConfiguration extends EntrypointClusterConfiguration {

  @Nonnull private final SavepointRestoreSettings savepointRestoreSettings;

  @Nullable private final JobID jobId;

  private final int parallelism;

  StatefulFunctionsClusterConfiguration(
      @Nonnull String configDir,
      @Nonnull Properties dynamicProperties,
      @Nonnull String[] args,
      @Nonnull SavepointRestoreSettings savepointRestoreSettings,
      @Nullable JobID jobId,
      int parallelism) {
    super(configDir, dynamicProperties, args);
    this.savepointRestoreSettings =
        requireNonNull(savepointRestoreSettings, "savepointRestoreSettings");
    this.jobId = jobId;
    this.parallelism = parallelism;
  }

  @Nonnull
  SavepointRestoreSettings getSavepointRestoreSettings() {
    return savepointRestoreSettings;
  }

  @Nullable
  JobID getJobId() {
    return jobId;
  }

  public int getParallelism() {
    return parallelism;
  }
}
