/*
 * Copyright 2026 The Strands Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.async.strands;

import static com.google.common.base.Preconditions.checkState;

import java.time.Duration;
import org.jspecify.annotations.Nullable;

/** A {@link Strand} that is always completed successfully with a result. */
final class ImmediateSuccessfulStrand<T extends @Nullable Object> extends AbstractStrand<T> {

  private final Scope scope;
  private final T value;

  ImmediateSuccessfulStrand(Scope scope, T value) {
    this.scope = scope;
    this.value = value;
  }

  /** Returns the current state of this Strand. */
  @Override
  public State state() {
    return State.SUCCEEDED;
  }

  /** Does nothing since this strand is already complete. */
  @Override
  public void cancel() {}

  /**
   * Immediately returns the result value of this Strand since this strand is already completed
   * successfully.
   *
   * @see Strand#await(Duration)
   * @throws IllegalStateException if called from outside the scope that created this strand
   */
  @Override
  public T await(Duration unused) {
    checkState(scope == Scope.current(), "Strand cannot be awaited from outside its scope.");
    return value;
  }

  /**
   * Returns the (always successful) {@link Result} of this Strand.
   *
   * @see AbstractStrand#result()
   */
  @Override
  protected Result<T> result() {
    return Result.ofValue(value);
  }

  @Override
  protected Scope scope() {
    return scope;
  }

  /**
   * Immediately returns {@code false} since the strand is already complete.
   *
   * @see AbstractStrand#pushCallback(ThreadSafeRunnable)
   */
  @Override
  protected boolean pushCallback(ThreadSafeRunnable unused) {
    return false;
  }
}
