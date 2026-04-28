// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.nettyclient;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.statefun.extensions.ExtensionModule;
import org.apache.flink.statefun.flink.core.httpfn.TransportClientConstants;

@AutoService(ExtensionModule.class)
public class NettyTransportModule implements ExtensionModule {

  @Override
  public void configure(Map<String, String> globalConfigurations, Binder binder) {
    binder.bindExtension(
        TransportClientConstants.ASYNC_CLIENT_FACTORY_TYPE,
        NettyRequestReplyClientFactory.INSTANCE);
  }
}
