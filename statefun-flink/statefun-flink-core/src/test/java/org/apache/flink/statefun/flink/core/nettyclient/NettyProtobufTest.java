// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.nettyclient;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntFunction;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.statefun.sdk.reqreply.generated.Address;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class NettyProtobufTest {

  @AfterEach
  public void tearDown() {
    ALLOCATOR.close();
  }

  private final AutoReleasingAllocator ALLOCATOR = new AutoReleasingAllocator();

  @Test
  public void roundTrip() {
    char[] chars = new char[1024 * 1024];
    Arrays.fill(chars, 'x');
    String pad = new String(chars);

    for (int i = 0; i < 100; i++) {
      int size = ThreadLocalRandom.current().nextInt(1, pad.length());
      Address original =
          Address.newBuilder()
              .setNamespace("namespace")
              .setType("type")
              .setId(pad.substring(0, size))
              .build();

      Address actual = serdeRoundTrip(ALLOCATOR, original);

      assertThat(actual, is(original));
    }
  }

  @Test
  public void heapBufferRoundTrip() {
    char[] chars = new char[1024 * 1024];
    Arrays.fill(chars, 'x');
    String pad = new String(chars);

    IntFunction<ByteBuf> heapAllocator = ByteBufAllocator.DEFAULT::heapBuffer;

    for (int i = 0; i < 100; i++) {
      int size = ThreadLocalRandom.current().nextInt(1, pad.length());
      Address original =
          Address.newBuilder()
              .setNamespace("namespace")
              .setType("type")
              .setId(pad.substring(0, size))
              .build();

      Address actual = serdeRoundTrip(heapAllocator, original);
      assertThat(actual, is(original));
    }
  }

  private Address serdeRoundTrip(IntFunction<ByteBuf> allocator, Address original) {
    ByteBuf buf = NettyProtobuf.serializeProtobuf(allocator, original);
    Address got = NettyProtobuf.deserializeProtobuf(buf, Address.parser());
    buf.release();
    return got;
  }

  private static final class AutoReleasingAllocator implements IntFunction<ByteBuf>, AutoCloseable {
    private final ArrayDeque<ByteBuf> allocatedDuringATest = new ArrayDeque<>();

    @Override
    public ByteBuf apply(int value) {
      ByteBuf buf = ByteBufAllocator.DEFAULT.directBuffer(value);
      allocatedDuringATest.addLast(buf);
      return buf;
    }

    @Override
    public void close() {
      for (ByteBuf buf : allocatedDuringATest) {
        int refCount = buf.refCnt();
        if (refCount > 0) {
          buf.release(refCount);
        }
      }
    }
  }
}
