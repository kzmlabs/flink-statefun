// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.java.handler;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

final class MoreFutures {

  @FunctionalInterface
  public interface Fn<I, O> {
    O apply(I input) throws Throwable;
  }

  /**
   * Apply @fn for each element of @elements sequentially. Subsequent element is handed to fn only
   * after the previous future has completed.
   */
  public static <T> CompletableFuture<Void> applySequentially(
      Iterable<T> elements, Fn<T, CompletableFuture<Void>> fn) {
    Objects.requireNonNull(elements);
    Objects.requireNonNull(fn);
    return applySequentially(elements.iterator(), fn);
  }

  private static <T> CompletableFuture<Void> applySequentially(
      Iterator<T> iterator, Fn<T, CompletableFuture<Void>> fn) {
    try {
      while (iterator.hasNext()) {
        T next = iterator.next();
        CompletableFuture<Void> future = fn.apply(next);
        if (!future.isDone()) {
          return future.thenCompose(ignored -> applySequentially(iterator, fn));
        }
        if (future.isCompletedExceptionally()) {
          return future;
        }
      }
      return CompletableFuture.completedFuture(null);
    } catch (Throwable t) {
      return exceptional(t);
    }
  }

  static <T> CompletableFuture<T> exceptional(Throwable cause) {
    CompletableFuture<T> e = new CompletableFuture<>();
    e.completeExceptionally(cause);
    return e;
  }
}
