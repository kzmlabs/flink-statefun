// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.io.kinesis.binders.ingress.v1;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.statefun.extensions.ExtensionModule;

@AutoService(ExtensionModule.class)
public final class Module implements ExtensionModule {

  @Override
  public void configure(Map<String, String> globalConfigurations, Binder universeBinder) {
    universeBinder.bindExtension(
        RoutableKinesisIngressBinderV1.KIND_TYPE, RoutableKinesisIngressBinderV1.INSTANCE);
  }
}
