// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.sdk.java;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * State Expiration Configuration
 *
 * <p>This class defines the way state can be auto expired by the runtime. State expiration (also
 * known as state TTL) can be used to keep state from growing arbitrarily by assigning an expiration
 * date to a value.
 *
 * <p>State can be expired after a duration had passed since either from the last write to the
 * state, or the last call to the function.
 */
public final class Expiration implements Serializable {

  private static final long serialVersionUID = 1L;

  public enum Mode {
    NONE,
    AFTER_WRITE,
    AFTER_CALL;
  }

  /**
   * Returns an {@link Expiration} configuration that would expire a @duration after the last write.
   *
   * @param duration a duration to wait before considering the state expired.
   */
  public static Expiration expireAfterWriting(Duration duration) {
    return new Expiration(Mode.AFTER_WRITE, duration);
  }

  /**
   * Returns an {@link Expiration} configuration that would expire a @duration after the last
   * invocation of the function.
   *
   * @param duration a duration to wait before considering the state expired.
   */
  public static Expiration expireAfterCall(Duration duration) {
    return new Expiration(Mode.AFTER_CALL, duration);
  }

  /**
   * Returns an {@link Expiration} configuration that has an expiration characteristic based on the
   * provided expire {@link Mode}.
   *
   * @param duration a duration to wait before considering the state expired.
   * @param mode the expire mode.
   */
  public static Expiration expireAfter(Duration duration, Mode mode) {
    return new Expiration(mode, duration);
  }

  /** @return Returns a disabled expiration */
  public static Expiration none() {
    return new Expiration(Mode.NONE, Duration.ZERO);
  }

  private final Mode mode;
  private final Duration duration;

  private Expiration(Mode mode, Duration duration) {
    this.mode = Objects.requireNonNull(mode);
    this.duration = Objects.requireNonNull(duration);
  }

  /** @return The expire mode of this {@link Expiration} configuration. */
  public Mode mode() {
    return mode;
  }

  /** @return The duration of this {@link Expiration} configuration. */
  public Duration duration() {
    return duration;
  }

  @Override
  public String toString() {
    return String.format("Expiration{mode=%s, duration=%s}", mode, duration);
  }
}
