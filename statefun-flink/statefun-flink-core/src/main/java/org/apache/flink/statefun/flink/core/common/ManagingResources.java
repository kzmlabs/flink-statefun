// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.common;

import org.apache.flink.annotation.Internal;

@Internal
public interface ManagingResources {
  /**
   * This method would be called by the runtime on shutdown, and indicates that this is the time to
   * free up any resources managed by this class.
   */
  void shutdown();
}
