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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.google.common.flogger.GoogleLogger;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.ThreadSafe;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;

/**
 * A {@link Strand} implementation that runs a task asynchronously by running it on a virtual thread
 * and handles state transitions for the lifetime of that task via an internal finite-state
 * automaton.
 *
 * <p>State, result, and thread are kept together in an immutable {@link Snapshot} updated
 * atomically via a {@link VarHandle}, ensuring the result is always consistent with the state and
 * state transitions are valid.
 *
 * <p>Only the following state transitions are allowed:
 *
 * <ul>
 *   <li>{@link State#CREATED} -> {@link State#READY}
 *   <li>{@link State#READY} -> {@link State#RUNNING}
 *   <li>{@link State#READY} -> {@link State#CANCELLED}
 *   <li>{@link State#READY} -> {@link State#TIMEOUT}
 *   <li>{@link State#READY} -> {@link State#INTERRUPTED}
 *   <li>{@link State#RUNNING} -> {@link State#SUCCEEDED}
 *   <li>{@link State#RUNNING} -> {@link State#FAILED}
 *   <li>{@link State#RUNNING} -> {@link State#CANCELLED}
 *   <li>{@link State#RUNNING} -> {@link State#TIMEOUT}
 *   <li>{@link State#RUNNING} -> {@link State#INTERRUPTED}
 * </ul>
 */
@ThreadSafe
final class AsyncStrand<T extends @Nullable Object> extends AbstractStrand<T> {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private record Snapshot<V extends @Nullable Object>(State state, @Nullable Result<V> result) {}

  private static final VarHandle CALLBACK_HANDLE;
  private static final VarHandle SNAPSHOT_HANDLE;

  static {
    try {
      Lookup lookup = MethodHandles.lookup();
      CALLBACK_HANDLE = lookup.findVarHandle(AsyncStrand.class, "callback", ThreadCallback.class);
      SNAPSHOT_HANDLE = lookup.findVarHandle(AsyncStrand.class, "snapshot", Snapshot.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final Scope scope;
  @SuppressWarnings("ThreadSafe") // VarHandle guarded.
  private volatile Snapshot<T> snapshot = new Snapshot<>(State.CREATED, null);
  @SuppressWarnings("ThreadSafe") // VarHandle guarded.
  private volatile @Nullable Thread thread;
  @SuppressWarnings("ThreadSafe") // VarHandle guarded.
  private volatile @Nullable ThreadCallback callback;
  @SuppressWarnings("ThreadSafe") // Guarded by state check.
  private final CancellableTask<T, ? extends Throwable> task;

  // Relative time for monitoring; each state transition takes reports the delta between this and
  // a new system time recorded when the state changes.
  //
  // When state is CREATED, this is the system time the Strand was created.
  // When state is READY, this is the system time the Strand was allocated a thread and it started.
  // When state is RUNNING, this is the system time the Strand started executing its task.
  // When state is SUCCEEDED, FAILED, CANCELLED, or TIMEOUT, this value should be ignored.
  @SuppressWarnings("ThreadSafe") // Guarded by state check.
  private volatile long lastStateTransitionNanos;

  public AsyncStrand(Scope scope, Task<T, ? extends Throwable> task) {
    this(scope, CancellableTask.withNoCancellation(task));
  }

  AsyncStrand(Scope scope, CancellableTask<T, ? extends Throwable> task) {
    this.scope = scope;
    this.task = task;
    this.lastStateTransitionNanos = fireStrandCreateEvent(scope);
  }

  @Override
  @SuppressWarnings("nullness") // compareAndSet accepts null.
  protected final boolean pushCallback(ThreadSafeRunnable runnable) {
    ThreadCallback current;
    ThreadCallback next;
    do {
      current = this.callback;
      if (state().isComplete || current == ThreadCallback.TOMBSTONE) {
        // Task has completed, so do not accept any more callbacks.
        return false;
      }
      next = new ThreadCallback(runnable, current);
    } while (!CALLBACK_HANDLE.compareAndSet(this, current, next));
    return true;
  }

  @Override
  protected final Scope scope() {
    return scope;
  }

  @Override
  public final void cancel() {
    if (tryTransitionToCancelled() == State.CANCELLED) {
      task.cancel();
    }
  }

  /**
   * Attempts to stop this Strand's task due to a timeout.
   *
   * <p>This behaves similarly to {@link #cancel()} except that rather than causing the Strand to
   * transition to the {@link State#CANCELLED} state, the Strand will transition to the {@link
   * State#TIMEOUT} state. If the Strand is already complete, this will have no effect.
   *
   * <p>This method is intended for internal use by the framework to allow more advanced timeout
   * handling.
   */
  final void timeout() {
    if (tryTransitionToTimeout() == State.TIMEOUT) {
      task.cancel();
    }
  }

  /**
   * Waits up to a timeout for the Strand's task to complete and returns its result.
   *
   * <p>If the Strand's task is already complete, this will return immediately.
   *
   * <p>If the Strand's task failed with <i>any</i> exception (including non-checked exceptions),
   * this will rethrow that exception wrapped in a {@link FailedTaskException}. If the {@code
   * timeout} is reached, this will throw a {@link FailedTaskException} with a {@link
   * TimeoutException} cause. If the {@code Strand} was cancelled, this will throw a {@link
   * FailedTaskException} with a {@link CancellationException} cause.
   *
   * @param timeout the maximum duration to wait for the Strand's task to complete, {@link
   *     Duration#ZERO} indicates there is no timeout and behaves the same as calling {@link
   *     #await()}
   * @throws FailedTaskException if the task failed with an exception rather than returning a result
   *     or the {@code timeout} was reached
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete; this is <i>not</i> the same as the task's thread being interrupted which
   *     results in a {@code FailedTaskException} will be thrown with an {@code
   *     InterruptedException} cause
   * @throws IllegalStateException if the Strand is not in the {@link State#READY} state
   * @throws IllegalStateException if called from outside the scope that created this Strand
   * @throws IllegalArgumentException if the {@code timeout} is negative
   */
  @Override
  public final T await(Duration timeout) throws InterruptedException {
    checkArgument(!timeout.isNegative(), "timeout must be non-negative: %s", timeout);
    checkState(Scope.current() == scope, "Strand cannot be awaited from outside its scope.");
    switch (state()) {
      case CREATED ->
          throw new StrandsInternalStateException(
              "await() called on a Strand that has not been started.");
      case TIMEOUT ->
          throw new StrandsInternalStateException(
              "await() called on a Strand that has already timed out.");
      default -> {}
    }

    Thread thread = this.thread;
    // If there is no thread, then the strand is already in some (valid)
    // terminal state, so just skip joining and return the result.
    if (thread != null) {
      try {
        if (timeout.isZero()) {
          // Wait indefinitely if there is no timeout.
          thread.join();
        } else {
          boolean terminated = thread.join(timeout);
          if (!terminated && !thread.isInterrupted()) {
            // Note: We can unconditionally attempt this transition because in the case where the
            // thread was joined successfully, the strand is guaranteed to be in a state where
            // this transition will have no effect.
            this.timeout();
            if (state() == State.TIMEOUT) {
              // Since the strand is now in the TIMEOUT state, we know that the thread is
              // interrupted and task has been cancelled, so we can just propagate the failure.
              throw new FailedTaskException(requireNonNull(result()).failure());
            }
          }
        }
      } catch (InterruptedException e) {
        // The current thread - not the strand's thread - was interrupted while waiting to join.
        // Interrupt the strand's thread since it's no longer needed and allow it to transition to
        // STOPPING.
        task.cancel();
        var _ = tryTransitionToInterrupted(e);
        Thread.currentThread().interrupt(); // Restore the interrupted status on the current thread.
        // Propagate the interruption. This does not set the strand's result or state as we want the
        // strand's thread itself to handle this. Since the current thread was interrupted, not
        // having
        // a result is expected.
        throw e;
      }
    }
    if (!state().isComplete) {
      throw new StrandsInternalStateException(
          "Thread join completed, but the Strand is not in a complete state %s", state());
    }
    Result<T> result = requireNonNull(result());
    if (result.isFailed()) {
      throw FailedTaskException.wrap(result.failure());
    }
    return result.value();
  }

  /** Allocates a new virtual thread for this Strand and starts it. */
  @CanIgnoreReturnValue
  @SuppressWarnings(
      "ReferenceEquality") // Identity comparison against singleton EventListener.EMPTY
  final Strand<T> start() {
    State current = state();
    if (current != State.CREATED) {
      throw new StrandsInternalStateException(
          "Strand cannot be started from the %s state (expected CREATED).", current);
    }
    fireStrandReadyEvent();
    var _ = tryTransitionToReady(() -> scope.startStrandThread(this::execute));
    return this;
  }

  @SuppressWarnings(
      "ReferenceEquality") // Identity comparison against singleton EventListener.EMPTY
  private void execute() {
    fireStrandStartEvent();

    // If the thread is marked as interrupted before the task starts, immediately transition to
    // INTERRUPTED. This will cause the state == state.RUNNING check to fail, and we'll skip to
    // running the callbacks and finishing.
    if (Thread.currentThread().isInterrupted()) {
      var _ =
          tryTransitionToInterrupted(
              new InterruptedException("Strand interrupted before execution started."));
    }

    State state = tryTransitionToRunning();
    // If the Strand was already cancelled or timed out before execution actually started, we won't
    // transition to RUNNING state, so we can skip to just running the callback and finishing.
    if (state == State.RUNNING) {
      // If any of these state transitions fail, it means the Strand was already cancelled or
      // timed out, or interrupted (and a result was already set).
      try {
        state = tryTransitionToSucceeded(task.run());
      } catch (Throwable e) {
        if (e instanceof InterruptedException ie) {
          Thread.currentThread().interrupt(); // Restore the interrupted status.
          state = tryTransitionToInterrupted(ie);
        } else {
          state = tryTransitionToFailed(e);
        }
      }
    }

    // Run all callbacks registered to this Strand.
    ThreadCallback callback =
        (ThreadCallback) CALLBACK_HANDLE.getAndSet(this, ThreadCallback.TOMBSTONE);
    if (callback != null && callback != ThreadCallback.TOMBSTONE) {
      try {
        callback.run();
      } catch (Throwable e) {
        logger.atWarning().atMostEvery(1, MINUTES).withCause(e).log(
            "Caught an exception while running a strand thread callback.");
      }
    }

    fireStrandCompleteEvent(state);
  }

  /** A task that can be cancelled. */
  interface CancellableTask<T extends @Nullable Object, X extends Throwable> extends Task<T, X> {
    /**
     * Returns the result of running this task.
     *
     * @throws X if the task fails
     */
    @Override
    T run() throws X;

    /** Cancels this task, cleaning up any resources it that are no longer needed. */
    void cancel();

    /**
     * Returns a {@link CancellableTask} that wraps the given {@link Task} (with no cancellation
     * behavior).
     */
    static <T extends @Nullable Object, X extends Throwable>
        CancellableTask<T, X> withNoCancellation(Task<T, X> task) {
      return new CancellableTask<T, X>() {
        @Override
        public T run() throws X {
          return task.run();
        }

        @Override
        public void cancel() {}
      };
    }

    /**
     * Returns a {@link CancellableTask} that wraps the given {@link Transform} (with no
     * cancellation behavior).
     */
    static <I extends @Nullable Object, O extends @Nullable Object, X extends Throwable>
        CancellableTask<O, X> withNoCancellation(
            I input, Transform<? super I, ? extends O, X> transform) {
      return new CancellableTask<O, X>() {
        @Override
        public O run() throws X {
          return transform.apply(input);
        }

        @Override
        public void cancel() {}
      };
    }
  }

  // =========================================================================
  // State Machine & Transitions
  // =========================================================================

  @Override
  public final State state() {
    return snapshot.state();
  }

  @Override
  protected final @Nullable Result<T> result() {
    return snapshot.result();
  }

  private static final Snapshot<@Nullable Object> READY_SNAPSHOT =
      new Snapshot<>(State.READY, null);
  private static final Snapshot<@Nullable Object> RUNNING_SNAPSHOT =
      new Snapshot<>(State.RUNNING, null);

  /**
   * Attempts to transition the strand to {@link State#READY} from {@link State#CREATED}, allocating
   * and starting the backing thread. If the transition fails, unregisters the strand from the
   * flock.
   */
  private State tryTransitionToReady(Supplier<Thread> threadSupplier) {
    if (SNAPSHOT_HANDLE.compareAndSet(this, snapshot, READY_SNAPSHOT)) {
      // Only allocate and start the thread if the transition to READY succeeds.
      thread = threadSupplier.get();
    }
    return snapshot.state();
  }

  /** Attempts to transition the strand to {@link State#RUNNING} from {@link State#READY}. */
  private State tryTransitionToRunning() {
    var _ = SNAPSHOT_HANDLE.compareAndSet(this, READY_SNAPSHOT, RUNNING_SNAPSHOT);
    return snapshot.state();
  }

  /** Attempts to transition the strand to {@link State#SUCCEEDED} from {@link State#RUNNING}. */
  private State tryTransitionToSucceeded(T value) {
    Snapshot<T> next = new Snapshot<>(State.SUCCEEDED, Result.ofValue(value));
    if (SNAPSHOT_HANDLE.compareAndSet(this, RUNNING_SNAPSHOT, next)) {
      thread = null;
    }
    return snapshot.state();
  }

  /** Attempts to transition the strand to {@link State#FAILED} from {@link State#RUNNING}. */
  private State tryTransitionToFailed(Throwable failure) {
    Snapshot<T> next = new Snapshot<>(State.FAILED, Result.ofFailure(failure));
    if (SNAPSHOT_HANDLE.compareAndSet(this, RUNNING_SNAPSHOT, next)) {
      thread = null;
    }
    return snapshot.state();
  }

  /** Attempts to transition the strand to {@link State#TIMEOUT}. */
  @SuppressWarnings("Interruption") // Intentionally interrupting the virtual thread.
  private State tryTransitionToTimeout() {
    Snapshot<T> next = new Snapshot<>(State.TIMEOUT, Result.ofFailure(new TimeoutException()));
    while (true) {
      Snapshot<T> current = snapshot;
      if (current.state() != State.READY && current.state() != State.RUNNING) {
        return current.state();
      }
      if (SNAPSHOT_HANDLE.compareAndSet(this, current, next)) {
        Thread thread = this.thread;
        if (thread != null) {
          thread.interrupt();
        }
        this.thread = null;
        return State.TIMEOUT;
      }
    }
  }

  /** Attempts to transition the strand to {@link State#CANCELLED}. */
  @SuppressWarnings("Interruption") // Intentionally interrupting the virtual thread.
  private State tryTransitionToCancelled() {
    Snapshot<T> next =
        new Snapshot<>(State.CANCELLED, Result.ofFailure(new CancellationException()));
    while (true) {
      Snapshot<T> current = snapshot;
      if (current.state() != State.READY && current.state() != State.RUNNING) {
        return current.state();
      }
      if (SNAPSHOT_HANDLE.compareAndSet(this, current, next)) {
        Thread thread = this.thread;
        if (thread != null) {
          thread.interrupt();
        }
        this.thread = null;
        return State.CANCELLED;
      }
    }
  }

  /** Attempts to transition the strand to {@link State#INTERRUPTED}. */
  @SuppressWarnings("Interruption") // Intentionally interrupting the virtual thread.
  private State tryTransitionToInterrupted(InterruptedException e) {
    var ie = new InterruptedException();
    ie.addSuppressed(e);
    Snapshot<T> next = new Snapshot<>(State.INTERRUPTED, Result.ofFailure(ie));
    while (true) {
      Snapshot<T> current = snapshot;
      if (current.state() != State.READY && current.state() != State.RUNNING) {
        return current.state();
      }
      if (SNAPSHOT_HANDLE.compareAndSet(this, current, next)) {
        Thread thread = this.thread;
        if (thread != null) {
          thread.interrupt();
        }
        this.thread = null;
        return State.INTERRUPTED;
      }
    }
  }

  /**
   * A thread-safe runnable that wraps a {@link ThreadSafeRunnable} and forms a stack of callbacks
   * to run in LIFO order.
   */
  @ThreadSafe
  private static final class ThreadCallback implements ThreadSafeRunnable {

    /**
     * A {@link ThreadCallback} that does nothing.
     *
     * <p>This is used as a sentinel value internally to differentiate between the absence of a
     * callback and a callback that has already been run and cleared.
     */
    private static final ThreadCallback TOMBSTONE = new ThreadCallback(() -> {}, null);

    private final ThreadSafeRunnable runnable;
    private final @Nullable ThreadCallback next;

    ThreadCallback(ThreadSafeRunnable runnable, @Nullable ThreadCallback next) {
      this.runnable = runnable;
      this.next = next;
    }

    @Override
    public void run() {
      for (ThreadCallback current = this; current != null; current = current.next) {
        try {
          current.runnable.run();
        } catch (Throwable e) {
          if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt(); // Restore the interrupted status.
          }
          logger.atWarning().atMostEvery(1, MINUTES).withCause(e).log(
              "Caught an exception while running a ThreadCallback.");
        }
      }
    }
  }

  @SuppressWarnings("ReferenceEquality") // Intentional identity comparison with EventListener.EMPTY
  private static final long fireStrandCreateEvent(Scope scope) {
    if (scope.context().eventListener() == EventListener.EMPTY) {
      return EventListener.UNTIMED_NANOS;
    }
    long createNanos =
        scope.context().timingMode() == TimingMode.FULL
            ? System.nanoTime()
            : EventListener.UNTIMED_NANOS;
    try {
      scope.context().eventListener().strandCreate();
    } catch (Throwable e) {
      // Reduces the size of this method to make it more likely for the JVM to inline it.
      handleStrandCreateInterruptAndWarn(e);
    }
    return createNanos;
  }

  /**
   * Logs a warning about the {@code strandCreate} event with the given exception and restores the
   * interrupted status of the current thread if necessary.
   *
   * <p>This method exists to reduce the size of {@link #fireStrandCreateEvent} to make it more
   * likely for the JVM to inline it, particularly because this exception path should be very rare.
   */
  private static final void handleStrandCreateInterruptAndWarn(Throwable e) {
    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt(); // Restore the interrupted status.
    }
    logger.atWarning().atMostEvery(1, MINUTES).withCause(e).log(
        "Caught an exception in response to an strandCreate event.");
  }

  @SuppressWarnings("ReferenceEquality") // Intentional identity comparison with EventListener.EMPTY
  private final void fireStrandReadyEvent() {
    if (scope.context().eventListener() == EventListener.EMPTY) {
      return;
    }
    boolean shouldTime = scope.context().timingMode() == TimingMode.FULL;
    long readyNanos = shouldTime ? System.nanoTime() : EventListener.UNTIMED_NANOS;
    long delayNanos =
        shouldTime ? readyNanos - lastStateTransitionNanos : EventListener.UNTIMED_NANOS;
    try {
      scope.context().eventListener().strandReady(delayNanos);
    } catch (Throwable e) {
      // Reduces the size of this method to make it more likely for the JVM to inline it.
      handleStrandReadyInterruptAndWarn(e);
    }
    lastStateTransitionNanos = readyNanos;
  }

  /**
   * Logs a warning about the {@code strandReady} event with the given exception and restores the
   * interrupted status of the current thread if necessary.
   *
   * <p>This method exists to reduce the size of {@link #fireStrandReadyEvent} to make it more
   * likely for the JVM to inline it, particularly because this exception path should be very rare.
   */
  private static final void handleStrandReadyInterruptAndWarn(Throwable e) {
    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt(); // Restore the interrupted status.
    }
    logger.atWarning().atMostEvery(1, MINUTES).withCause(e).log(
        "Caught an exception in response to an strandReady event.");
  }

  @SuppressWarnings("ReferenceEquality") // Intentional identity comparison with EventListener.EMPTY
  private final void fireStrandStartEvent() {
    if (scope.context().eventListener() == EventListener.EMPTY) {
      return;
    }
    boolean shouldTime = scope.context().timingMode() == TimingMode.FULL;
    long startNanos = shouldTime ? System.nanoTime() : EventListener.UNTIMED_NANOS;
    long delayNanos =
        shouldTime ? startNanos - lastStateTransitionNanos : EventListener.UNTIMED_NANOS;
    try {
      scope.context().eventListener().strandStart(delayNanos);
    } catch (Throwable e) {
      // Reduces the size of this method to make it more likely for the JVM to inline it.
      handleStrandStartInterruptAndWarn(e);
    }
    lastStateTransitionNanos = startNanos;
  }

  /**
   * Logs a warning about the {@code strandStart} event with the given exception and restores the
   * interrupted status of the current thread if necessary.
   *
   * <p>This method exists to reduce the size of {@link #fireStrandStartEvent} to make it more
   * likely for the JVM to inline it, particularly because this exception path should be very rare.
   */
  private static final void handleStrandStartInterruptAndWarn(Throwable e) {
    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt(); // Restore the interrupted status.
    }
    logger.atWarning().atMostEvery(1, MINUTES).withCause(e).log(
        "Caught an exception in response to an strandStart event.");
  }

  @SuppressWarnings("ReferenceEquality") // Intentional identity comparison with EventListener.EMPTY
  private final void fireStrandCompleteEvent(State state) {
    if (scope.context().eventListener() == EventListener.EMPTY) {
      return;
    }
    boolean shouldTime = scope.context().timingMode() == TimingMode.FULL;
    long completeNanos = shouldTime ? System.nanoTime() : EventListener.UNTIMED_NANOS;
    long runtimeNanos =
        shouldTime ? completeNanos - lastStateTransitionNanos : EventListener.UNTIMED_NANOS;
    try {
      scope.context().eventListener().strandComplete(runtimeNanos, state);
    } catch (Throwable e) {
      // Reduces the size of this method to make it more likely for the JVM to inline it.
      handleStrandCompleteInterruptAndWarn(e);
    }
  }

  /**
   * Logs a warning about the {@code strandComplete} event with the given exception and restores the
   * interrupted status of the current thread if necessary.
   *
   * <p>This method exists to reduce the size of {@link #fireStrandCompleteEvent} to make it more
   * likely for the JVM to inline it, particularly because this exception path should be very rare.
   */
  private static final void handleStrandCompleteInterruptAndWarn(Throwable e) {
    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt(); // Restore the interrupted status.
    }
    logger.atWarning().atMostEvery(1, MINUTES).withCause(e).log(
        "Caught an exception in response to an strandComplete event.");
  }
}
