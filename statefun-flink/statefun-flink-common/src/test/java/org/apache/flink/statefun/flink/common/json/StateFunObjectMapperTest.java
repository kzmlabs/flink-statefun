// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.common.json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.sdk.TypeName;
import org.junit.jupiter.api.Test;

class StateFunObjectMapperTest {

  private final ObjectMapper mapper = StateFunObjectMapper.create();

  @Test
  void durationSerializesUsingFlinkTimeUtilsHighestUnit() throws Exception {
    String json = mapper.writeValueAsString(new DurationHolder(Duration.ofMinutes(5)));

    // Flink's TimeUtils.formatWithHighestUnit picks the largest unit that is exact;
    // 5 minutes -> "5 min".
    assertThat(json).contains("\"value\":\"5 min\"");
  }

  @Test
  void durationDeserializesMatchingFlinkTimeUtilsParse() throws Exception {
    DurationHolder holder = mapper.readValue("{\"value\":\"30 s\"}", DurationHolder.class);

    assertThat(holder.value).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void durationRoundtripPreservesValue() throws Exception {
    DurationHolder in = new DurationHolder(Duration.ofMillis(750));
    String json = mapper.writeValueAsString(in);
    DurationHolder out = mapper.readValue(json, DurationHolder.class);

    assertThat(out.value).isEqualTo(in.value);
  }

  @Test
  void typeNameDeserializesFromNamespaceNameString() throws Exception {
    TypeNameHolder holder =
        mapper.readValue(
            "{\"value\":\"io.test/transport-client\"}", TypeNameHolder.class);

    assertThat(holder.value).isEqualTo(TypeName.parseFrom("io.test/transport-client"));
  }

  @Test
  void unknownPropertiesAreIgnoredByDefault() throws Exception {
    // FAIL_ON_UNKNOWN_PROPERTIES is disabled; previously-unknown fields shouldn't blow up.
    DurationHolder holder =
        mapper.readValue(
            "{\"value\":\"1 s\",\"unknown\":\"ignored\"}", DurationHolder.class);

    assertThat(holder.value).isEqualTo(Duration.ofSeconds(1));
  }

  @Test
  void invalidDurationStringFailsCleanly() {
    assertThatThrownBy(
            () -> mapper.readValue("{\"value\":\"not-a-duration\"}", DurationHolder.class))
        // Jackson wraps the underlying IllegalArgumentException from TimeUtils.parseDuration
        // — accept either to stay robust against jackson-shaded classification quirks.
        .satisfiesAnyOf(
            t -> assertThat(t).isInstanceOf(IllegalArgumentException.class),
            t -> assertThat(t).isInstanceOf(JsonMappingException.class));
  }

  static final class DurationHolder {
    public Duration value;

    public DurationHolder() {}

    DurationHolder(Duration value) {
      this.value = value;
    }
  }

  static final class TypeNameHolder {
    public TypeName value;

    public TypeNameHolder() {}
  }
}
