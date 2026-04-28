// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.functions;

import org.apache.flink.statefun.sdk.FunctionType;

public interface FunctionRepository {

  LiveFunction get(FunctionType type);
}
