// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.launcher;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.junit.jupiter.api.Test;

class StatefulFunctionsClusterEntryPointTest {

  @Test
  void resolveJobIdReturnsExplicitlyProvidedJobId() {
    JobID provided = JobID.generate();

    JobID resolved =
        StatefulFunctionsClusterEntryPoint.resolveJobIdForCluster(
            Optional.of(provided), new Configuration());

    assertThat(resolved).isEqualTo(provided);
  }

  @Test
  void resolveJobIdReturnsZeroWhenHighAvailabilityIsEnabled() {
    Configuration cfg = new Configuration();
    // HA enabled — under HA we want a stable JobID across restarts so that checkpoints
    // can be matched back. The launcher uses the all-zeros JobID as that stable handle.
    cfg.set(HighAvailabilityOptions.HA_MODE, "zookeeper");

    JobID resolved =
        StatefulFunctionsClusterEntryPoint.resolveJobIdForCluster(Optional.empty(), cfg);

    assertThat(resolved).isEqualTo(StatefulFunctionsClusterEntryPoint.ZERO_JOB_ID);
  }

  @Test
  void resolveJobIdGeneratesFreshIdWhenHighAvailabilityIsDisabled() {
    Configuration cfg = new Configuration();
    cfg.set(HighAvailabilityOptions.HA_MODE, "NONE");

    JobID first =
        StatefulFunctionsClusterEntryPoint.resolveJobIdForCluster(Optional.empty(), cfg);
    JobID second =
        StatefulFunctionsClusterEntryPoint.resolveJobIdForCluster(Optional.empty(), cfg);

    assertThat(first).isNotNull();
    assertThat(second).isNotNull();
    // Two consecutive generations of a non-HA job must produce different IDs, otherwise
    // two parallel cluster starts would collide.
    assertThat(first).isNotEqualTo(second);
  }

  @Test
  void setDefaultExecutionModeWritesDetachedWhenUnset() {
    Configuration cfg = new Configuration();

    StatefulFunctionsClusterEntryPoint.setDefaultExecutionModeIfNotConfigured(cfg);

    assertThat(cfg.get(ClusterEntrypoint.INTERNAL_CLUSTER_EXECUTION_MODE, null))
        .isEqualTo(ClusterEntrypoint.ExecutionMode.DETACHED.toString());
  }

  @Test
  void setDefaultExecutionModeDoesNotOverrideExistingConfiguration() {
    Configuration cfg = new Configuration();
    cfg.set(
        ClusterEntrypoint.INTERNAL_CLUSTER_EXECUTION_MODE,
        ClusterEntrypoint.ExecutionMode.NORMAL.toString());

    StatefulFunctionsClusterEntryPoint.setDefaultExecutionModeIfNotConfigured(cfg);

    assertThat(cfg.get(ClusterEntrypoint.INTERNAL_CLUSTER_EXECUTION_MODE, null))
        .isEqualTo(ClusterEntrypoint.ExecutionMode.NORMAL.toString());
  }

  @Test
  void zeroJobIdIsAllZeros() {
    assertThat(StatefulFunctionsClusterEntryPoint.ZERO_JOB_ID)
        .isEqualTo(new JobID(0L, 0L));
  }
}
