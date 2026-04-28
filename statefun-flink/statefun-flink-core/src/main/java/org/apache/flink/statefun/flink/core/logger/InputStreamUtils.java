// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.logger;

import java.io.IOException;
import java.io.InputStream;
import org.apache.flink.util.Preconditions;

final class InputStreamUtils {

  private InputStreamUtils() {}

  /**
   * Attempt to fill the provided read buffer with bytes from the given {@link InputStream}, and
   * returns the total number of bytes read into the read buffer.
   *
   * <p>This method repeatedly reads the {@link InputStream} until either:
   *
   * <ul>
   *   <li>the read buffer is filled, or
   *   <li>EOF of the input stream is reached.
   * </ul>
   *
   * @param in the input stream to read from
   * @param readBuffer the read buffer to fill
   * @return the total number of bytes read into the read buffer
   */
  static int tryReadFully(final InputStream in, final byte[] readBuffer) throws IOException {
    Preconditions.checkState(readBuffer.length > 0, "read buffer size must be larger than 0.");

    int totalRead = 0;
    while (totalRead != readBuffer.length) {
      int read = in.read(readBuffer, totalRead, readBuffer.length - totalRead);
      if (read == -1) {
        break;
      }
      totalRead += read;
    }
    return totalRead;
  }
}
