// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.logger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.InputStream;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public final class InputStreamUtilsTest {

  private enum InputStreamType {
    RANDOM_LENGTH_PER_READ,
    ONE_BYTE_PER_READ
  }

  static Stream<InputStreamType> testInputStreamTypes() {
    return Stream.of(InputStreamType.RANDOM_LENGTH_PER_READ, InputStreamType.ONE_BYTE_PER_READ);
  }

  @ParameterizedTest
  @MethodSource("testInputStreamTypes")
  void tryReadFullyExampleUsage(InputStreamType testInputStreamType) throws Exception {
    final byte[] testBytes = "test-data".getBytes();
    final byte[] readBuffer = new byte[testBytes.length];

    try (InputStream in = testInputStream(testInputStreamType, testBytes)) {
      final int numReadBytes = InputStreamUtils.tryReadFully(in, readBuffer);

      assertThat(numReadBytes, is(testBytes.length));
      assertThat(readBuffer, is(testBytes));
      assertThat(in.read(), is(-1));
    }
  }

  @ParameterizedTest
  @MethodSource("testInputStreamTypes")
  void tryReadFullyEmptyInputStream(InputStreamType testInputStreamType) throws Exception {
    final byte[] testBytes = new byte[0];
    final byte[] readBuffer = new byte[10];

    try (InputStream in = testInputStream(testInputStreamType, testBytes)) {
      final int numReadBytes = InputStreamUtils.tryReadFully(in, readBuffer);

      assertThat(numReadBytes, is(0));
      assertThat(readBuffer, is(new byte[10]));
      assertThat(in.read(), is(-1));
    }
  }

  @ParameterizedTest
  @MethodSource("testInputStreamTypes")
  void tryReadFullyReadBufferSizeLargerThanInputStream(InputStreamType testInputStreamType)
      throws Exception {
    final byte[] testBytes = new byte[] {-91, 11, 8};
    // read buffer has larger size than the test data
    final byte[] readBuffer = new byte[testBytes.length + 20];

    try (InputStream in = testInputStream(testInputStreamType, testBytes)) {
      final int numReadBytes = InputStreamUtils.tryReadFully(in, readBuffer);

      assertThat(numReadBytes, is(testBytes.length));
      assertThat(readBuffer, is(Arrays.copyOf(testBytes, readBuffer.length)));
      assertThat(in.read(), is(-1));
    }
  }

  @ParameterizedTest
  @MethodSource("testInputStreamTypes")
  void tryReadFullyReadBufferSizeSmallerThanInputStream(InputStreamType testInputStreamType)
      throws Exception {
    final byte[] testBytes = new byte[] {-91, 11, 8, 53, 100, 5, -100, 102, 56, 95};
    // read buffer has smaller size than the test data
    final byte[] readBuffer = new byte[testBytes.length - 2];

    try (InputStream in = testInputStream(testInputStreamType, testBytes)) {
      final int numReadBytes = InputStreamUtils.tryReadFully(in, readBuffer);

      assertThat(numReadBytes, is(readBuffer.length));
      assertThat(readBuffer, is(Arrays.copyOfRange(testBytes, 0, readBuffer.length)));

      // assert that the input stream is not overly-read
      assertThat(in.read(), is(56));
      assertThat(in.read(), is(95));
      assertThat(in.read(), is(-1));
    }
  }

  @ParameterizedTest
  @MethodSource("testInputStreamTypes")
  void tryReadFullyEmptyReadBuffer(InputStreamType testInputStreamType) {
    assertThrows(
        IllegalStateException.class,
        () ->
            InputStreamUtils.tryReadFully(
                testInputStream(testInputStreamType, "test-data".getBytes()), new byte[0]));
  }

  private static InputStream testInputStream(
      InputStreamType testInputStreamType, byte[] streamBytes) {
    switch (testInputStreamType) {
      case ONE_BYTE_PER_READ:
        return new OneBytePerReadByteArrayInputStream(
            Arrays.copyOf(streamBytes, streamBytes.length));
      default:
      case RANDOM_LENGTH_PER_READ:
        return new RandomReadLengthByteArrayInputStream(
            Arrays.copyOf(streamBytes, streamBytes.length));
    }
  }
}
