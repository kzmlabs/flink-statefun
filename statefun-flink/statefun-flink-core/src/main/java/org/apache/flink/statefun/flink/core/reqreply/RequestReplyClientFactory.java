// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.core.reqreply;

import java.net.URI;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

@PublicEvolving
public interface RequestReplyClientFactory {
  RequestReplyClient createTransportClient(ObjectNode transportProperties, URI endpointUrl);

  void cleanup();
}
