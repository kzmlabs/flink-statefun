// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.core.httpfn;

import java.io.Serializable;
import java.net.URI;
import java.util.Objects;
import org.apache.flink.statefun.sdk.FunctionType;

public final class UrlPathTemplate implements Serializable {
  private static final long serialVersionUID = 1;

  private static final String FUNCTION_NAME_HOLDER = "{function.name}";

  private final String template;

  public UrlPathTemplate(String template) {
    this.template = Objects.requireNonNull(template);
  }

  public URI apply(FunctionType functionType) {
    return URI.create(template.replace(FUNCTION_NAME_HOLDER, functionType.name()));
  }
}
