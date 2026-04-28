// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.common;

import java.io.Serializable;
import java.util.function.Function;

public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {}
