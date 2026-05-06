// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Kzmlabs
package org.apache.flink.statefun.flink.core.httpfn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import org.apache.flink.statefun.sdk.FunctionType;
import org.junit.jupiter.api.Test;

class UrlPathTemplateTest {

  @Test
  void substitutesFunctionNameHolder() {
    UrlPathTemplate template = new UrlPathTemplate("http://upstream/api/{function.name}");

    URI uri = template.apply(new FunctionType("counter", "increment"));

    assertThat(uri.toString()).isEqualTo("http://upstream/api/increment");
  }

  @Test
  void leavesTemplateAsIsWhenHolderIsAbsent() {
    UrlPathTemplate template = new UrlPathTemplate("http://upstream/api/static");

    URI uri = template.apply(new FunctionType("anything", "anything"));

    assertThat(uri.toString()).isEqualTo("http://upstream/api/static");
  }

  @Test
  void substitutesAllOccurrencesOfHolder() {
    UrlPathTemplate template =
        new UrlPathTemplate("http://upstream/{function.name}/handle/{function.name}");

    URI uri = template.apply(new FunctionType("ns", "fn"));

    assertThat(uri.toString()).isEqualTo("http://upstream/fn/handle/fn");
  }

  @Test
  void doesNotSubstituteNamespace() {
    // Confirms holder substitution uses the function NAME, not namespace.
    UrlPathTemplate template = new UrlPathTemplate("/x/{function.name}");

    URI uri = template.apply(new FunctionType("namespace-value", "name-value"));

    assertThat(uri.getPath()).isEqualTo("/x/name-value");
  }

  @Test
  void rejectsNullTemplateAtConstruction() {
    assertThatThrownBy(() -> new UrlPathTemplate(null)).isInstanceOf(NullPointerException.class);
  }
}
