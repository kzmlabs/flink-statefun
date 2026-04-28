// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.jsonmodule;

enum FormatVersion {
  // ============================================================
  //  EOL versions
  // ============================================================

  v1_0("1.0"),
  v2_0("2.0"),

  // ============================================================
  //  Supported versions
  // ============================================================

  v3_0("3.0"),
  v3_1("3.1");

  private String versionStr;

  FormatVersion(String versionStr) {
    this.versionStr = versionStr;
  }

  @Override
  public String toString() {
    return versionStr;
  }

  static FormatVersion fromString(String versionStr) {
    switch (versionStr) {
      case "1.0":
        return v1_0;
      case "2.0":
        return v2_0;
      case "3.0":
        return v3_0;
      case "3.1":
        return v3_1;
      default:
        throw new IllegalArgumentException("Unrecognized format version: " + versionStr);
    }
  }
}
