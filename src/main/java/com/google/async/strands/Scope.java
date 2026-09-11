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

import static java.util.concurrent.TimeUnit.MINUTES;

import com.google.common.flogger.GoogleLogger;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.ThreadSafe;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Represents a point in execution flow where execution can split into separate Strands and be
 * awaited on.
 *
 * <p>This is the fundamental unit of structured concurrency in Strands. Using this class, you may
 * start parallel branches of work (Strands, each its own virtual thread) that have well-defined
 * semantics over what happens if a Strand in the group fails, or completes early, or any number of
 * execution timing behaviors. As each Strand executes within a call to {@link Strands#concurrent},
 * they have a single-execution guarantee between each other such that only one will execute at a
 * time, allowing straightforward sharing of mutable data and reasoning around execution logic.
 */
@ThreadSafe
final class Scope implements AutoCloseable {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /**
   * The {@link ScopedValue} holding the current {@link Scope} for the thread.
   *
   * <p>Propagated explicitly to child virtual threads in {@link #startStrandThread} and {@link
   * #startFrameworkInternalThread} via {@link ScopedValue#where(ScopedValue, Object)}.
   */
  private static final ScopedValue<Scope> CURRENT_SCOPE = ScopedValue.newInstance();

  /** Returns the current {@link Scope} for the current thread. */
  static Scope current() {
    if (!CURRENT_SCOPE.isBound()) {
      throw new IllegalStateException(
          "No Strands scope available, did you call this from outside Strands.concurrent()?");
    }
    return CURRENT_SCOPE.get();
  }

  /**
   * Runs the given {@code task} in a new {@link Scope} with the given {@link ExecutionContext}.
   *
   * <p>This is the point of control for choosing which environment to run Strands with; everything
   * within this scope will use the provided boundary.
   *
   * @throws X if the task failed with an exception rather than returning a result
   */
  static <T extends @Nullable Object, X extends Throwable> T run(
      ExecutionContext context, Task<T, X> task) throws X {
    Scope scope = new Scope(context);
    // Run the task in a ScopedValue context to make the scope available to any Strands created in
    // the task.
    return ScopedValue.where(CURRENT_SCOPE, scope)
        .<T, X>call(
            () -> {
              long openNanos = scope.fireScopeOpenEvent();
              try {
                return task.run();
              } finally {
                try {
                  scope.close();
                } finally {
                  scope.fireScopeCloseEvent(openNanos);
                }
              }
            });
  }

  private final ExecutionContext context;
  private final ScopeThreadFlock flock;

  @SuppressWarnings("nullness") // 'this' is under initialization when computing identity string.
  private Scope(ExecutionContext context) {
    this.context = context;
    this.flock = new ScopeThreadFlock(Objects.toIdentityString(this));
  }

  /** Returns the {@link ExecutionContext} for this scope. */
  ExecutionContext context() {
    return context;
  }

  /** Returns the {@link EventListener} for this scope. */
  EventListener listener() {
    return context.listener();
  }

  @Override
  public void close() {
    try {
      flock.shutdown();
      flock.interruptAll();
    } finally {
      flock.close();
    }
  }

  /**
   * Creates and starts a new virtual thread for the given task bound to this scope and subject to
   * the scope's single-execution guarantee.
   *
   * <p>All thread creation should use this method unless there is a specific reason the
   * single-execution guarantee should not apply. See {@link #startFrameworkInternalThread}.
   */
  @CanIgnoreReturnValue
  Thread startStrandThread(Runnable task) {
    ContextPropagationOperator operator = context.contextPropagationOperator();
    Runnable targetTask = operator != null ? operator.apply(task) : task;
    Runnable scopedTask =
        () -> {
          try {
            ScopedValue.where(CURRENT_SCOPE, this).run(targetTask);
          } finally {
            flock.unregister(Thread.currentThread());
          }
        };
    Thread thread = null;
    try {
      thread = context.taskThreadFactory().newVirtualThread(scopedTask);
      flock.register(thread);
      thread.start();
    } catch (Throwable t) {
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt(); // Preserve interrupted status
      }
      if (thread != null) {
        // If we managed to create the thread, but failed to start it, just immediately unregister
        // the thread to avoid a dangling reference. If we also failed to register the thread, this
        // is a no-op.
        flock.unregister(thread);
      }
      throw t;
    }
    return thread;
  }

  /**
   * Creates and starts a framework internal task on a new virtual thread bound to this scope, but
   * <b>not</b> subject to the scope's single-execution guarantee.
   *
   * <p>This method is intended for framework-internal workloads that can operate outside the bounds
   * of the scope's single-execution guarantee, but which should still be bound to the scope for
   * purposes of scope closure. These threads do not propagate context from the surrounding scope.
   *
   * <p>For example, {@link UnorderedGatherer} and {@link OrderedGatherer} use this method to start
   * threads for handling timeouts to avoid threads backing Strands blocking promptly marking slow
   * Strands as timed out.
   *
   * <p>This method <b>must</b> be used instead of creating a new virtual thread directly (e.g. via
   * {@link Thread#ofVirtual()}) as this method will ensure the thread is properly registered with
   * the scope and adhere to the framework's structured concurrency semantics.
   */
  @CanIgnoreReturnValue
  Thread startFrameworkInternalThread(Runnable task) {
    Thread thread = null;
    Runnable scopedTask =
        () -> {
          try {
            ScopedValue.where(CURRENT_SCOPE, this).run(task);
          } finally {
            flock.unregister(Thread.currentThread());
          }
        };
    try {
      thread = context.frameworkThreadFactory().newVirtualThread(scopedTask);
      flock.register(thread);
      thread.start();
    } catch (Throwable t) {
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt(); // Preserve interrupted status
      }
      if (thread != null) {
        // If we managed to create the thread, but failed to start it, just immediately unregister
        // the thread to avoid a dangling reference. If we also failed to register the thread, this
        // is a no-op.
        flock.unregister(thread);
      }
      throw t;
    }
    return thread;
  }

  @SuppressWarnings("ReferenceEquality") // Intentional identity comparison with EventListener.EMPTY
  private final long fireScopeOpenEvent() {
    if (context.listener() == EventListener.EMPTY) {
      return EventListener.UNTIMED_NANOS;
    }
    long openNanos =
        context.timingMode() == TimingMode.FULL ? System.nanoTime() : EventListener.UNTIMED_NANOS;
    try {
      context.listener().scopeOpen();
    } catch (Throwable e) {
      // Reduces the size of this method to make it more likely for the JVM to inline it.
      handleScopeOpenInterruptAndWarn(e);
    }
    return openNanos;
  }

  /**
   * Logs a warning about the {@code scopeOpen} event with the given exception and restores the
   * interrupted status of the current thread if necessary.
   *
   * <p>This method exists to reduce the size of {@link #fireScopeOpenEvent} to make it more likely
   * for the JVM to inline it, particularly because this exception path should be very rare.
   */
  private static final void handleScopeOpenInterruptAndWarn(Throwable e) {
    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt(); // Restore the interrupted status.
    }
    logger.atWarning().atMostEvery(1, MINUTES).withCause(e).log(
        "Caught an exception in response to an scopeOpen event.");
  }

  @SuppressWarnings("ReferenceEquality") // Intentional identity comparison with EventListener.EMPTY
  private final void fireScopeCloseEvent(long openNanos) {
    if (context.listener() == EventListener.EMPTY) {
      return;
    }
    long durationNanos =
        context.timingMode() == TimingMode.FULL
            ? System.nanoTime() - openNanos
            : EventListener.UNTIMED_NANOS;
    try {
      context.listener().scopeClose(durationNanos);
    } catch (Throwable e) {
      // Reduces the size of this method to make it more likely for the JVM to inline it.
      handleScopeCloseInterruptAndWarn(e);
    }
  }

  /**
   * Logs a warning about the {@code scopeClose} event with the given exception and restores the
   * interrupted status of the current thread if necessary.
   *
   * <p>This method exists to reduce the size of {@link #fireScopeCloseEvent} to make it more likely
   * for the JVM to inline it, particularly because this exception path should be very rare.
   */
  private static final void handleScopeCloseInterruptAndWarn(Throwable e) {
    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt(); // Restore the interrupted status.
    }
    logger.atWarning().atMostEvery(1, MINUTES).withCause(e).log(
        "Caught an exception in response to an scopeClose event.");
  }
}
