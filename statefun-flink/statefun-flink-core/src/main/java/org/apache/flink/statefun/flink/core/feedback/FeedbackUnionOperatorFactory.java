// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.flink.core.feedback;

import java.util.Objects;
import java.util.OptionalLong;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.common.SerializableFunction;
import org.apache.flink.streaming.api.operators.*;

public final class FeedbackUnionOperatorFactory<E> implements OneInputStreamOperatorFactory<E, E> {

  private static final long serialVersionUID = 1;

  private final StatefulFunctionsConfig configuration;

  private final FeedbackKey<E> feedbackKey;
  private final SerializableFunction<E, OptionalLong> isBarrierMessage;
  private final SerializableFunction<E, ?> keySelector;

  public FeedbackUnionOperatorFactory(
      StatefulFunctionsConfig configuration,
      FeedbackKey<E> feedbackKey,
      SerializableFunction<E, OptionalLong> isBarrierMessage,
      SerializableFunction<E, ?> keySelector) {
    this.feedbackKey = Objects.requireNonNull(feedbackKey);
    this.isBarrierMessage = Objects.requireNonNull(isBarrierMessage);
    this.keySelector = Objects.requireNonNull(keySelector);
    this.configuration = Objects.requireNonNull(configuration);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends StreamOperator<E>> T createStreamOperator(
      StreamOperatorParameters<E> streamOperatorParameters) {
    final TypeSerializer<E> serializer =
        streamOperatorParameters
            .getStreamConfig()
            .getTypeSerializerIn(
                0, streamOperatorParameters.getContainingTask().getUserCodeClassLoader());

    FeedbackUnionOperator<E> op =
        new FeedbackUnionOperator<>(
            feedbackKey,
            isBarrierMessage,
            keySelector,
            configuration.getFeedbackBufferSize().getBytes(),
            serializer,
            streamOperatorParameters);

    return (T) op;
  }

  @Override
  public void setChainingStrategy(ChainingStrategy chainingStrategy) {
    // ignored
  }

  @Override
  public ChainingStrategy getChainingStrategy() {
    return ChainingStrategy.ALWAYS;
  }

  @Override
  public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
    return FeedbackUnionOperator.class;
  }
}
