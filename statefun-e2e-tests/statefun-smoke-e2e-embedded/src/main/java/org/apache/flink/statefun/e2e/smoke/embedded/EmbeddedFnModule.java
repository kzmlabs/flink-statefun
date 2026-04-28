// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.e2e.smoke.embedded;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.statefun.e2e.smoke.SmokeRunnerParameters;
import org.apache.flink.statefun.e2e.smoke.driver.Constants;
import org.apache.flink.statefun.e2e.smoke.driver.Ids;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(StatefulFunctionModule.class)
public class EmbeddedFnModule implements StatefulFunctionModule {
  public static final Logger LOG = LoggerFactory.getLogger(EmbeddedFnModule.class);

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    SmokeRunnerParameters parameters = SmokeRunnerParameters.from(globalConfiguration);
    LOG.info(parameters.toString());

    Ids ids = new Ids(parameters.getNumberOfFunctionInstances());

    FunctionProvider provider = new FunctionProvider(ids);
    binder.bindFunctionProvider(Constants.FN_TYPE, provider);
  }
}
