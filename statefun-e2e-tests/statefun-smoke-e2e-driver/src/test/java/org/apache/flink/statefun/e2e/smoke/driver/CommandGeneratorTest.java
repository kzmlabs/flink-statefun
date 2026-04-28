// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.e2e.smoke.driver;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.flink.statefun.e2e.smoke.SmokeRunnerParameters;
import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;
import org.junit.jupiter.api.Test;

public class CommandGeneratorTest {

  @Test
  public void usageExample() {
    SmokeRunnerParameters parameters = new SmokeRunnerParameters();
    parameters.setAsyncOpSupported(true);
    CommandGenerator generator = new CommandGenerator(new JDKRandomGenerator(), parameters);

    SourceCommand command = generator.get();

    assertThat(command.getTarget(), notNullValue());
    assertThat(command.getCommands(), notNullValue());
  }
}
