// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.core.nettyclient;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.net.InetSocketAddress;
import java.net.URI;
import org.junit.jupiter.api.Test;

public class EndpointTest {

  @Test
  public void exampleUsage() {
    Endpoint endpoint = new Endpoint(URI.create("https://api.gateway.com:1234/statefun?xyz=5678"));

    assertThat(endpoint.useTls(), is(true));
    assertThat(endpoint.serviceAddress().getHostString(), is("api.gateway.com"));
    assertThat(endpoint.serviceAddress().getPort(), is(1234));
    assertThat(endpoint.queryPath(), is("/statefun?xyz=5678"));
  }

  @Test
  public void anotherExample() {
    Endpoint endpoint = new Endpoint(URI.create("https://greeter-svc/statefun"));

    assertThat(endpoint.useTls(), is(true));
    assertThat(endpoint.queryPath(), is("/statefun"));

    InetSocketAddress serviceAddress = endpoint.serviceAddress();
    assertThat(serviceAddress.getHostString(), is("greeter-svc"));
    assertThat(serviceAddress.getPort(), is(443));
  }

  @Test
  public void emptyQueryPathIsASingleSlash() {
    Endpoint endpoint = new Endpoint(URI.create("http://greeter-svc"));

    assertThat(endpoint.queryPath(), is("/"));
  }

  @Test
  public void dontUseTls() {
    Endpoint endpoint = new Endpoint(URI.create("http://api.gateway.com:1234/statefun?xyz=5678"));

    assertThat(endpoint.useTls(), is(false));
  }

  @Test
  public void useTls() {
    Endpoint endpoint = new Endpoint(URI.create("https://foobar.net"));

    assertThat(endpoint.useTls(), is(true));
  }
}
