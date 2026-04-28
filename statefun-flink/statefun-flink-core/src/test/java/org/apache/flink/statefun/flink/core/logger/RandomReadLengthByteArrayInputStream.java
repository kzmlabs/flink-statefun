// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Random;

/**
 * A {@link ByteArrayInputStream} that reads a random number of bytes per read (at least 1) up to
 * the requested amount, and always returns 0 on {@link InputStream#available()}.
 *
 * <p>We use this input stream in our tests to mimic behaviour of "real-life" input streams, while
 * still adhering to the contracts of the {@link InputStream} methods:
 *
 * <ul>
 *   <li>For {@link InputStream#read(byte[])} and {@link InputStream#read()}: read methods always
 *       blocks until at least 1 byte is available from the stream; it always at least reads 1 byte.
 *   <li>For {@link InputStream#available()}: always return 0, to imply that there are no bytes
 *       immediately available from the stream, and the next read will block.
 * </ul>
 */
final class RandomReadLengthByteArrayInputStream extends ByteArrayInputStream {

  private static final Random RANDOM = new Random();

  RandomReadLengthByteArrayInputStream(byte[] byteBuffer) {
    super(byteBuffer);
  }

  @Override
  public int read(byte[] b, int off, int len) {
    final int randomNumBytesToRead = RANDOM.nextInt(len) + 1;
    return super.read(b, off, randomNumBytesToRead);
  }

  @Override
  public synchronized int available() {
    return 0;
  }
}
