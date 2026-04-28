// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.functions;

import org.apache.flink.statefun.sdk.FunctionType;

public interface FunctionRepository {

  LiveFunction get(FunctionType type);
}
