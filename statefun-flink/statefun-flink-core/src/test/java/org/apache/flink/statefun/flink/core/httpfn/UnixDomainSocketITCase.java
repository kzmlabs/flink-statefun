// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.httpfn;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import javax.net.ServerSocketFactory;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.newsclub.net.unix.AFUNIXServerSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

public class UnixDomainSocketITCase {

  private static boolean isWindows() {
    return System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("windows");
  }

  @BeforeEach
  public void skipOnWindows() {
    // Unix domain sockets are not supported on Windows
    assumeFalse(isWindows(), "Skipping Unix domain socket test on Windows");
  }

  @Test
  @Timeout(value = 10 * 1_000, unit = TimeUnit.MILLISECONDS)
  public void unixDomainSocket() throws IOException {
    final File sockFile = new File("/tmp/uds-" + System.nanoTime() + ".sock");
    sockFile.deleteOnExit();

    try (MockWebServer server = new MockWebServer()) {
      server.setServerSocketFactory(udsSocketFactory(sockFile));
      server.enqueue(new MockResponse().setBody("hi"));
      server.start();

      OkHttpClient client = udsSocketClient(sockFile);

      Response response = request(client);

      assertTrue(response.isSuccessful());
      assertThat(response.body(), is(notNullValue()));
      assertThat(response.body().string(), is("hi"));
    }
  }

  private static Response request(OkHttpClient client) throws IOException {
    Request request = new Request.Builder().url("http://unused/").build();
    return client.newCall(request).execute();
  }

  /** returns an {@link OkHttpClient} that connects trough the provided socket file. */
  private static OkHttpClient udsSocketClient(File sockFile) {
    Builder sharedClient = OkHttpUtils.newClient().newBuilder();
    OkHttpUnixSocketBridge.configureUnixDomainSocket(sharedClient, sockFile);
    return sharedClient.build();
  }

  private static ServerSocketFactory udsSocketFactory(File sockFile) {
    return new ServerSocketFactory() {
      @Override
      public ServerSocket createServerSocket() throws IOException {
        return AFUNIXServerSocket.forceBindOn(new AFUNIXSocketAddress(sockFile));
      }

      @Override
      public ServerSocket createServerSocket(int i) throws IOException {
        return createServerSocket();
      }

      @Override
      public ServerSocket createServerSocket(int i, int i1) throws IOException {
        return createServerSocket();
      }

      @Override
      public ServerSocket createServerSocket(int i, int i1, InetAddress inetAddress)
          throws IOException {
        return createServerSocket();
      }
    };
  }
}
