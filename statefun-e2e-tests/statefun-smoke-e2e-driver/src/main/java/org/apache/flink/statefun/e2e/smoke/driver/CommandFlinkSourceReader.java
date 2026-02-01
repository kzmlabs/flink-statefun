/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.statefun.e2e.smoke.driver;

import static org.apache.flink.statefun.e2e.smoke.driver.Types.packSourceCommand;

import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.statefun.e2e.smoke.SmokeRunnerParameters;
import org.apache.flink.statefun.e2e.smoke.generated.Command;
import org.apache.flink.statefun.e2e.smoke.generated.Commands;
import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;
import org.apache.flink.statefun.e2e.smoke.generated.SourceSnapshot;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandFlinkSourceReader implements SourceReader<TypedValue, CommandFlinkSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(CommandFlinkSourceReader.class);

  private transient FunctionStateTracker functionStateTracker;
  private transient int commandsSentSoFar;
  private transient int failuresSoFar;
  private transient boolean done;
  private transient boolean generated;
  private transient boolean atLeastOneCheckpointCompleted;
  private final SmokeRunnerParameters parameters;

  public CommandFlinkSourceReader(SmokeRunnerParameters parameters) {
    this.parameters = parameters;
  }

  @Override
  public void start() {
    // TODO recover state
    SourceSnapshot sourceSnapshot = SourceSnapshot.getDefaultInstance();
    functionStateTracker =
        new FunctionStateTracker(this.parameters.getNumberOfFunctionInstances())
            .apply(sourceSnapshot.getTracker());
    commandsSentSoFar = sourceSnapshot.getCommandsSentSoFarHandle();
    failuresSoFar = sourceSnapshot.getFailuresGeneratedSoFar();
  }

  @Override
  public InputStatus pollNext(ReaderOutput<TypedValue> readerOutput) {
    if (!generated) {
      generated = true;
      generate(readerOutput);
    }

    snooze();
    verify(readerOutput);

    return (done) ? InputStatus.END_OF_INPUT : InputStatus.MORE_AVAILABLE;
  }

  @Override
  public List<CommandFlinkSourceSplit> snapshotState(long checkpointId) {
    return List.of();
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void addSplits(List<CommandFlinkSourceSplit> list) {}

  @Override
  public void notifyNoMoreSplits() {}

  @Override
  public void close() {
    done = true;
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    this.atLeastOneCheckpointCompleted = true;
  }

  private void generate(ReaderOutput<TypedValue> readerOutput) {
    final int startPosition = this.commandsSentSoFar;
    final OptionalInt kaboomIndex =
        computeFailureIndex(startPosition, failuresSoFar, parameters.getMaxFailures());
    if (kaboomIndex.isPresent()) {
      failuresSoFar++;
    }
    LOG.info(
        "starting at {}, kaboom at {}, total messages {}, random command generator seed {}",
        startPosition,
        kaboomIndex,
        parameters.getMessageCount(),
        parameters.getRandomGeneratorSeed());

    RandomGenerator random = new JDKRandomGenerator();
    random.setSeed(parameters.getRandomGeneratorSeed());
    Supplier<SourceCommand> generator = new CommandGenerator(random, parameters);

    FunctionStateTracker functionStateTracker = this.functionStateTracker;
    for (int i = startPosition; i < parameters.getMessageCount(); i++) {
      if (atLeastOneCheckpointCompleted && kaboomIndex.isPresent() && i >= kaboomIndex.getAsInt()) {
        throw new RuntimeException("KABOOM!!!");
      }
      SourceCommand command = generator.get();
      if (done) {
        return;
      }
      functionStateTracker.apply(command);
      readerOutput.collect(packSourceCommand(command));
      this.commandsSentSoFar = i;
    }
  }

  private void verify(ReaderOutput<TypedValue> readerOutput) {
    FunctionStateTracker functionStateTracker = this.functionStateTracker;

    for (int i = 0; i < parameters.getNumberOfFunctionInstances(); i++) {
      final long expected = functionStateTracker.stateOf(i);

      Command.Builder verify =
          Command.newBuilder().setVerify(Command.Verify.newBuilder().setExpected(expected));

      SourceCommand command =
          SourceCommand.newBuilder()
              .setTarget(i)
              .setCommands(Commands.newBuilder().addCommand(verify))
              .build();
      readerOutput.collect(packSourceCommand(command));
    }
  }

  private OptionalInt computeFailureIndex(int startPosition, int failureSoFar, int maxFailures) {
    if (failureSoFar >= maxFailures) {
      return OptionalInt.empty();
    }
    if (startPosition >= parameters.getMessageCount()) {
      return OptionalInt.empty();
    }
    int index = ThreadLocalRandom.current().nextInt(startPosition, parameters.getMessageCount());
    return OptionalInt.of(index);
  }

  private static void snooze() {
    try {
      Thread.sleep(2_000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
