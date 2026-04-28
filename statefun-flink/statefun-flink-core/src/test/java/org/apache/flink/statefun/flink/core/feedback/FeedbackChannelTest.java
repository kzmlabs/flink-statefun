// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.feedback;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@SuppressWarnings({
  "SameParameterValue",
  "IOResourceOpenedButNotSafelyClosed",
  "resource",
  "IOResourceOpenedButNotSafelyClosed",
  "unused"
})
public class FeedbackChannelTest {
  private static final SubtaskFeedbackKey<String> KEY =
      new FeedbackKey<String>("foo", 1).withSubTaskIndex(0, 1);

  @Test
  public void exampleUsage() {
    FeedbackChannel<String> channel =
        new FeedbackChannel<>(KEY, new LockFreeBatchFeedbackQueue<>());
    channel.put("hello");
    channel.put("world");
    channel.close();

    ArrayList<String> results = new ArrayList<>();

    channel.registerConsumer(results::add, Runnable::run);

    assertThat(results, contains("hello", "world"));
  }

  @Disabled("benchmarks are not run as part of a regular test suite.")
  @Test
  public void launchBenchmark() throws Exception {
    // The following is the result of comparing 3 different queue implementations for a spsc
    // this is the result of concurrently producing and consuming 1 million items.
    // The other implementations are not checkpointed into the code base to avoid accidental
    // megamorphic call sites.
    //
    // Benchmark                                             Mode  Cnt   Score    Error  Units
    // HandOffFeedbackChannelTest.lockBasedHandOffQueue      avgt    4  55.284 ± 43.094  ms/op
    // HandOffFeedbackChannelTest.lockFreeBatchHandOffQueue  avgt    4  17.411 ±  3.968  ms/op
    // HandOffFeedbackChannelTest.lockFreeStackBasedQueue    avgt    4  42.569 ±  8.616  ms/op
    //
    Options opt =
        new OptionsBuilder()
            .include(this.getClass().getName() + ".*")
            .timeUnit(TimeUnit.MILLISECONDS)
            .warmupIterations(4)
            .measurementIterations(4)
            .threads(2)
            .forks(1)
            .shouldFailOnError(true)
            .shouldDoGC(true)
            .build();

    new Runner(opt).run();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void lockFreeBatchHandOffQueue(Blackhole blackhole) {
    int bh = benchmark(new LockFreeBatchFeedbackQueue<>(), 1_000_000);
    blackhole.consume(bh);
  }

  private static int benchmark(FeedbackQueue<String> queue, int items) {
    FeedbackChannel<String> channel = new FeedbackChannel<>(KEY, queue);
    //
    // consumer
    //
    int[] consumed = new int[1];
    Object lock = new Object();
    ExecutorService executor =
        Executors.newSingleThreadExecutor(
            runnable -> {
              Thread t = new Thread(runnable);
              t.setDaemon(true);
              return t;
            });

    channel.registerConsumer(unused -> consumed[0]++, executor);
    //
    // producer
    //
    for (int i = 0; i < items; i++) {
      channel.put("hello");
    }
    channel.close();

    return consumed[0];
  }
}
