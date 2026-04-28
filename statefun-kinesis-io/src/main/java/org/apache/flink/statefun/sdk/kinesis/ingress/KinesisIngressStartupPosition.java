// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.sdk.kinesis.ingress;

import java.time.ZonedDateTime;

/** Position for the ingress to start consuming AWS Kinesis shards. */
public abstract class KinesisIngressStartupPosition {

  private KinesisIngressStartupPosition() {}

  /** Start consuming from the earliest position possible. */
  public static KinesisIngressStartupPosition fromEarliest() {
    return EarliestPosition.INSTANCE;
  }

  /** Start consuming from the latest position, i.e. head of the stream shards. */
  public static KinesisIngressStartupPosition fromLatest() {
    return LatestPosition.INSTANCE;
  }

  /**
   * Start consuming from position with ingestion timestamps after or equal to a specified {@link
   * ZonedDateTime}.
   */
  public static KinesisIngressStartupPosition fromDate(ZonedDateTime date) {
    return new DatePosition(date);
  }

  /** Checks whether this position is configured using the earliest position. */
  public final boolean isEarliest() {
    return getClass() == EarliestPosition.class;
  }

  /** Checks whether this position is configured using the latest position. */
  public final boolean isLatest() {
    return getClass() == LatestPosition.class;
  }

  /** Checks whether this position is configured using a date. */
  public final boolean isDate() {
    return getClass() == DatePosition.class;
  }

  /** Returns this position as a {@link DatePosition}. */
  public final DatePosition asDate() {
    if (!isDate()) {
      throw new IllegalStateException("This is not a startup position configured using a date.");
    }
    return (DatePosition) this;
  }

  @SuppressWarnings("WeakerAccess")
  public static final class EarliestPosition extends KinesisIngressStartupPosition {
    private static final EarliestPosition INSTANCE = new EarliestPosition();
  }

  @SuppressWarnings("WeakerAccess")
  public static final class LatestPosition extends KinesisIngressStartupPosition {
    private static final LatestPosition INSTANCE = new LatestPosition();
  }

  public static final class DatePosition extends KinesisIngressStartupPosition {

    private final ZonedDateTime date;

    private DatePosition(ZonedDateTime date) {
      this.date = date;
    }

    public ZonedDateTime date() {
      return date;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof DatePosition)) {
        return false;
      }

      DatePosition that = (DatePosition) obj;
      return that.date.equals(date);
    }

    @Override
    public int hashCode() {
      return date.hashCode();
    }
  }
}
