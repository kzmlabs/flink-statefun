// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.statefun.sdk.match;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;

/**
 * A {@link StatefulMatchFunction} is an utility {@link StatefulFunction} that supports pattern
 * matching on function inputs to decide how the inputs should be processed.
 *
 * <p>Please see {@link MatchBinder} for the supported types of pattern matching.
 *
 * @see MatchBinder
 */
public abstract class StatefulMatchFunction implements StatefulFunction {

  private boolean setup = false;

  private MatchBinder matcher = new MatchBinder();

  /**
   * Configures the patterns to match for the function's inputs.
   *
   * @param binder a {@link MatchBinder} to bind patterns on.
   */
  public abstract void configure(MatchBinder binder);

  @Override
  public final void invoke(Context context, Object input) {
    ensureInitialized();
    matcher.invoke(context, input);
  }

  private void ensureInitialized() {
    if (!setup) {
      setup = true;
      configure(matcher);
    }
  }
}
