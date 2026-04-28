// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.httpfn;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.statefun.extensions.ExtensionModule;

@AutoService(ExtensionModule.class)
public class TransportClientsModule implements ExtensionModule {
  @Override
  public void configure(Map<String, String> globalConfigurations, Binder binder) {
    binder.bindExtension(
        TransportClientConstants.OKHTTP_CLIENT_FACTORY_TYPE,
        DefaultHttpRequestReplyClientFactory.INSTANCE);
  }
}
