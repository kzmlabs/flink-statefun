// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.datastream;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.statefun.flink.core.httpfn.DefaultHttpRequestReplyClientFactory;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionEndpointSpec;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionProvider;
import org.apache.flink.statefun.flink.core.httpfn.TransportClientConstants;
import org.apache.flink.statefun.flink.core.nettyclient.NettyRequestReplyClientFactory;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClientFactory;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.TypeName;

@NotThreadSafe
@Internal
final class SerializableHttpFunctionProvider implements SerializableStatefulFunctionProvider {

  private static final long serialVersionUID = 1;

  private final HttpFunctionEndpointSpec spec;
  private transient @Nullable HttpFunctionProvider delegate;

  SerializableHttpFunctionProvider(HttpFunctionEndpointSpec spec) {
    this.spec = Objects.requireNonNull(spec);
  }

  @Override
  public StatefulFunction functionOfType(FunctionType type) {
    if (delegate == null) {
      delegate =
          new HttpFunctionProvider(spec, getClientFactory(spec.transportClientFactoryType()));
    }
    return delegate.functionOfType(type);
  }

  @VisibleForTesting
  static RequestReplyClientFactory getClientFactory(TypeName factoryType) {
    if (factoryType.equals(TransportClientConstants.OKHTTP_CLIENT_FACTORY_TYPE)) {
      return DefaultHttpRequestReplyClientFactory.INSTANCE;
    } else if (factoryType.equals(TransportClientConstants.ASYNC_CLIENT_FACTORY_TYPE)) {
      return NettyRequestReplyClientFactory.INSTANCE;
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported transport client factory type: %s",
              factoryType.canonicalTypenameString()));
    }
  }
}
