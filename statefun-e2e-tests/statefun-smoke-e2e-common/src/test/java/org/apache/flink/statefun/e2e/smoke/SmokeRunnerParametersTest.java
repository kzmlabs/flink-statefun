// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.e2e.smoke;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class SmokeRunnerParametersTest {

  @Test
  public void exampleUsage() {
    Map<String, String> keys = Collections.singletonMap("messageCount", "1");
    SmokeRunnerParameters parameters = SmokeRunnerParameters.from(keys);

    assertThat(parameters.getMessageCount(), is(1));
  }

  @Test
  public void roundTrip() {
    SmokeRunnerParameters original = new SmokeRunnerParameters();
    original.setCommandDepth(1234);

    SmokeRunnerParameters deserialized = SmokeRunnerParameters.from(original.asMap());

    assertThat(deserialized.getCommandDepth(), is(1234));
  }
}
