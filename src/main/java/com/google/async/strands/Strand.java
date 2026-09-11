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

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import org.jspecify.annotations.Nullable;

/** Represents the execution of a virtual thread that returns a value. */
public sealed interface Strand<T extends @Nullable Object> permits AbstractStrand {

  /** Returns the current state of this Strand. */
  State state();

  /** Cancels an incomplete Strand. */
  void cancel();

  /**
   * Waits indefinitely for the Strand's task to complete and returns its result.
   *
   * <p>If the Strand's task is already complete, this will return immediately.
   *
   * <p>If the Strand's task failed with <i>any</i> exception (including non-checked exceptions),
   * this will rethrow that exception wrapped in a {@link FailedTaskException}. The {@code Strand}
   * will have a {@link State#FAILED} state, except if the {@code Strand} was cancelled with {@link
   * #cancel()}, in which case the {@code Strand} will have a {@link State#CANCELLED} state and this
   * will throw a {@link FailedTaskException} with a {@link
   * java.util.concurrent.CancellationException} cause.
   *
   * @throws FailedTaskException if the task failed with an exception rather than returning a result
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete
   * @throws IllegalStateException if called from anywhere other than the scope that created this
   *     Strand
   */
  default T await() throws InterruptedException {
    return await(Duration.ZERO);
  }

  /**
   * Waits with a {@code timeout} for the task to complete and returns its result.
   *
   * <p>If the Strand's task failed with <i>any</i> exception (including non-checked exceptions),
   * this will rethrow that exception wrapped in a {@link FailedTaskException}. The {@code Strand}
   * will have a {@link State#FAILED} state, except if either of the following apply:
   *
   * <ul>
   *   <li>The {@code timeout} was reached, in which case the {@code Strand} will have a {@link
   *       State#TIMEOUT} state and this will throw a {@link FailedTaskException} with a {@link
   *       java.util.concurrent.TimeoutException} cause.
   *   <li>The {@code Strand} was cancelled with {@link #cancel()}, in which case the {@code Strand}
   *       will have a {@link State#CANCELLED} state and this will throw a {@link
   *       FailedTaskException} with a {@link java.util.concurrent.CancellationException} cause.
   * </ul>
   *
   * <p>If the Strand's task is already complete, this will return immediately.
   *
   * @param timeout the maximum duration to wait for the Strand's task to complete, {@link
   *     Duration#ZERO} indicates there is no timeout and behaves the same as calling {@link
   *     #await()}
   * @throws FailedTaskException if the task failed with an exception rather than returning a
   *     result, the {@code timeout} was reached, or the {@code Strand} was cancelled
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete; this is <i>not</i> the same as the task's thread being interrupted which
   *     results in a {@code FailedTaskException} will be thrown with an {@code
   *     InterruptedException} cause
   * @throws IllegalStateException if called from anywhere other than the scope that created this
   *     Strand
   */
  T await(Duration timeout) throws InterruptedException;

  /**
   * Waits indefinitely for the Strand's task to complete and returns a {@link Result} that wraps
   * the successful result or the exception it failed with.
   *
   * <p>If the Strand's task failed with <i>any</i> exception (including non-checked exceptions),
   * the {@code Result} will contain that exception. The {@code Strand} will have a {@link
   * State#FAILED} state, except if the {@code Strand} was cancelled with {@link #cancel()}, in
   * which case the {@code Strand} will have a {@link State#CANCELLED} state and the {@code Result}
   * will contain a {@link java.util.concurrent.CancellationException}.
   *
   * <p>If the Strand's task is already complete, this will return immediately.
   *
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete
   * @throws IllegalStateException if called from anywhere other than the scope that created this
   *     Strand
   */
  default Result<T> awaitResult() throws InterruptedException {
    try {
      return Result.ofValue(await());
    } catch (FailedTaskException e) {
      return Result.ofFailure(requireNonNull(e.getCause()));
    }
  }

  /**
   * Waits up to a timeout for the Strand's task to complete and returns a {@link Result} that wraps
   * the successful result or the exception it failed with.
   *
   * <p>If the Strand's task failed with <i>any</i> exception (including non-checked exceptions),
   * the {@code Result} will contain that exception, except if either of the following apply:
   *
   * <ul>
   *   <li>The {@code timeout} was reached, in which case the {@code Strand} will have a {@link
   *       State#TIMEOUT} state and the {@code Result} will contain a {@link TimeoutException}.
   *   <li>The {@code Strand} was cancelled with {@link #cancel()}, in which case the {@code Strand}
   *       will have a {@link State#CANCELLED} state and the {@code Result} will contain a {@link
   *       java.util.concurrent.CancellationException}.
   * </ul>
   *
   * <p>If the Strand's task is already complete, this will return immediately.
   *
   * @param timeout the maximum duration to wait for the Strand's task to complete, {@link
   *     Duration#ZERO} indicates there is no timeout and behaves the same as calling {@link
   *     #awaitResult()}
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete
   * @throws IllegalStateException if called from anywhere other than the scope that created this
   *     Strand
   */
  default Result<T> awaitResult(Duration timeout) throws InterruptedException {
    try {
      return Result.ofValue(await(timeout));
    } catch (FailedTaskException e) {
      return Result.ofFailure(requireNonNull(e.getCause()));
    }
  }

  /** State of the Strand. */
  public enum State {
    /** The strand has been created, but has not been allocated a virtual thread. */
    CREATED(false),
    /** The Strand has been allocated a virtual thread and is ready to run. */
    READY(false),
    /** The Strand is executing its task. */
    RUNNING(false),
    /** The Strand has completed successfully and has a result value. */
    SUCCEEDED(true),
    /**
     * The Strand has completed with an exception other than the following:
     *
     * <ul>
     *   <li>{@link CancellationException} (see {@link State#CANCELLED})
     *   <li>{@link TimeoutException} (see {@link State#TIMEOUT})
     *   <li>{@link InterruptedException} (see {@link State#INTERRUPTED})
     * </ul>
     */
    FAILED(true),
    /**
     * The Strand was cancelled with {@link #cancel()} and is failed with a {@link
     * CancellationException}.
     */
    CANCELLED(true),
    /**
     * The Strand hit its timeout while running {@link #await(Duration)} and is failed with a {@link
     * TimeoutException}.
     */
    TIMEOUT(true),
    /**
     * The Strand's thread was interrupted while running and is failed with an {@link
     * InterruptedException}.
     *
     * <p>This is distinct from {@link #CANCELLED} in that the Strand's thread was interrupted while
     * running its task rather than as a result of calling {@link #cancel()}.
     */
    INTERRUPTED(true);

    private State(boolean isComplete) {
      this.isComplete = isComplete;
    }

    /** Whether or not this state represents a terminal state (success or failure). */
    final boolean isComplete;
  }
}
