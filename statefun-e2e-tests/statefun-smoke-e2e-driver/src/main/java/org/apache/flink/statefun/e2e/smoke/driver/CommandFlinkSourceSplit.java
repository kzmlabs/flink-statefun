// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.e2e.smoke.driver;

import java.io.Serializable;
import org.apache.flink.api.connector.source.SourceSplit;

public class CommandFlinkSourceSplit implements SourceSplit, Serializable {
  private static final long serialVersionUID = 1L;

  private final int id;

  public CommandFlinkSourceSplit(int id) {
    this.id = id;
  }

  @Override
  public String splitId() {
    return Integer.toString(id);
  }
}
