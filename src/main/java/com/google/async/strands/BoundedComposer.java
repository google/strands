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

import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;

import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;

/**
 * A intermediate object to allow users to create a {@link Strand} that is the result of a bounded
 * number of {@link Strand} candidates.
 *
 * <p>This is a one-shot object and can only be consumed once. Once consumed, no other methods may
 * be called on it.
 */
public final class BoundedComposer<T extends @Nullable Object> {
  private final Scope scope;
  private final Strand<T>[] candidates;
  private boolean consumed;

  /**
   * Returns a new {@code BoundedComposer} for one or more candidate {@code Strands}.
   *
   * @param scope the {@link Scope} the composer and any {@code Strands} it creates belong to
   * @throws IllegalArgumentException if no candidates are provided
   * @throws IllegalArgumentException if any {@code candidates} do not belong to the {@code scope}
   */
  BoundedComposer(Scope scope, Strand<T>... candidates) {
    if (candidates.length == 0) {
      throw new IllegalArgumentException("At least one candidate must be provided");
    }
    for (int i = 0; i < candidates.length; i++) {
      if (((AbstractStrand<T>) candidates[i]).scope() != scope) {
        throw new IllegalArgumentException(
            String.format("candidates[%d] does not belong to the same scope as the composer", i));
      }
    }
    this.scope = scope;
    this.candidates = candidates.clone();
    this.consumed = false;
  }

  /**
   * Returns a {@link Strand} that is the result of the first candidate to complete successfully
   * when executed in the current scope; any remaining candidates are cancelled. If all candidates
   * fail, the failure of the first candidate to fail is captured in the returned {@code Strand}
   * with the failures of the other candidates as suppressed exceptions.
   *
   * <p>Candidates are already started by this point and within a single-execution boundary, only
   * one will be executing at any time, so if the first candidate started never blocks and completes
   * successfully, the others will be cancelled before running.
   *
   * <p>This is a <b>terminal</b> method and consumes the composer. Once this is called, no other
   * methods may be called on the composer.
   *
   * @throws IllegalStateException if called from outside the scope that created this composer
   * @throws IllegalStateException if the caller already consumed this composer by calling a
   *     terminal method (e.g. {@link #firstSuccessful()})
   */
  public final Strand<T> firstSuccessful() {
    if (consumed) {
      throw new IllegalStateException("this composer has already been consumed");
    }
    if (Scope.current() != scope) {
      throw new IllegalStateException("called from outside the scope that created this composer");
    }
    consumed = true;
    for (Strand<T> candidate : candidates) {
      Result<T> result = ((AbstractStrand<T>) candidate).result();
      if (result != null && result.isOk()) {
        // A candidate already succeeded, cancel everything and return the result.
        for (Strand<T> toCancel : candidates) {
          toCancel.cancel();
        }
        return new ImmediateSuccessfulStrand<>(scope, result.value());
      }
    }
    return new AsyncStrand<T>(scope, new FirstSuccessfulTask<T>(candidates)).start();
  }

  /**
   * Returns a {@link Strand} that collects all of the results of candidate {@code Strands} into a
   * {@link Stream} that maintains the order the candidates were provided. If a candidate {@code
   * Strand} fails, the returned {@code Strand} will fail with the first failure encountered from
   * any of the candidates and all remaining {@code Strands} are cancelled.
   *
   * <p>If you need more advanced stream processing, consider {@link FluentGatherer}.
   *
   * <p>This is a <b>terminal</b> method and consumes the composer. Once this is called, no other
   * methods may be called on the composer.
   *
   * @throws IllegalStateException if called from outside the scope that created this composer
   * @throws IllegalStateException if the caller already consumed this composer by calling a
   *     terminal method (e.g. {@link #firstSuccessful()})
   */
  public final Strand<Stream<T>> allSuccessful() {
    if (consumed) {
      throw new IllegalStateException("this composer has already been consumed");
    }
    if (Scope.current() != scope) {
      throw new IllegalStateException("called from outside the scope that created this composer");
    }
    consumed = true;
    boolean complete = true;
    for (Strand<T> candidate : candidates) {
      Result<T> result = ((AbstractStrand<T>) candidate).result();
      if (result == null) {
        complete = false;
        continue;
      }
      if (result.isFailed()) {
        // A candidate already failed, cancel everything and return the failure.
        for (Strand<T> toCancel : candidates) {
          toCancel.cancel();
        }
        return new ImmediateFailedStrand<>(scope, result.failure());
      }
    }
    if (complete) {
      // All candidates already completed successfully.
      return new ImmediateSuccessfulStrand<>(
          scope,
          stream(candidates).map(s -> requireNonNull(((AbstractStrand<T>) s).result()).value()));
    }
    return new AsyncStrand<Stream<T>>(scope, new AllSuccessfulTask<T>(candidates)).start();
  }

  /**
   * Returns a {@link Strand} that is the result of collecting all of the {@link Result}s of
   * candidate {@code Strand}s into a {@link Stream} that maintains the order the candidates were
   * provided. The returned {@code Strand} will not complete until all candidates have completed.
   *
   * <p>If you need more advanced stream processing, consider {@link FluentGatherer}.
   *
   * <p>This is a <b>terminal</b> method and consumes the composer. Once this is called, no other
   * methods may be called on the composer.
   *
   * @throws IllegalStateException if called from outside the scope that created this composer
   * @throws IllegalStateException if the caller already consumed this composer by calling a
   *     terminal method (e.g. {@link #firstSuccessful()})
   */
  public final Strand<Stream<Result<T>>> allCompleted() {
    if (consumed) {
      throw new IllegalStateException("this composer has already been consumed");
    }
    if (Scope.current() != scope) {
      throw new IllegalStateException("called from outside the scope that created this composer");
    }
    consumed = true;
    boolean complete = true;
    for (Strand<T> candidate : candidates) {
      Result<T> result = ((AbstractStrand<T>) candidate).result();
      if (result == null) {
        complete = false;
        break;
      }
    }
    if (complete) {
      // All candidates already completed.
      return new ImmediateSuccessfulStrand<>(
          scope,
          stream(candidates).<Result<T>>map(s -> requireNonNull(((AbstractStrand<T>) s).result())));
    }
    return new AsyncStrand<Stream<Result<T>>>(scope, new AllCompletedTask<T>(candidates)).start();
  }
}
