// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.harness.io;

import java.io.Serializable;
import java.util.function.Consumer;

public interface SerializableConsumer<T> extends Serializable, Consumer<T> {}
