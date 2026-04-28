// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.queue;

public interface Lock {

  void lockUninterruptibly();

  void unlock();
}
