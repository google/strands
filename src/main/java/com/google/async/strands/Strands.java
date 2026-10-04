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

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.stream.Gatherer;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;

/** Collection of methods for launching new nested scopes of Strands. */
public final class Strands {

  /** Returns {@code true} if the calling thread is currently executing within a Strands scope. */
  public static boolean inScope() {
    return Scope.isBound();
  }

  /**
   * Returns a {@link Strand} that is the result of the given {@link Task} executed in the current
   * scope.
   *
   * @throws IllegalStateException if called from outside the task passed to {@link #concurrent}
   */
  public static <T extends @Nullable Object> Strand<T> async(Task<T, ? extends Throwable> task) {
    return new AsyncStrand<T>(Scope.current(), task).start();
  }

  /**
   * Returns a {@link Strand} that is the result of the given {@link Future} executed in the current
   * scope.
   *
   * <p>NOTE: Strands has no ability to propagate context to futures. The caller is responsible for
   * handling context propagation to futures, e.g. via the Executor the future was created with.
   *
   * @throws IllegalStateException if called from outside the task passed to {@link #concurrent}
   */
  public static <T extends @Nullable Object> Strand<T> async(Future<T> future) {
    return switch (future.state()) {
      case Future.State.SUCCESS ->
          new ImmediateSuccessfulStrand<T>(Scope.current(), future.resultNow());
      case Future.State.CANCELLED ->
          new ImmediateFailedStrand<T>(Scope.current(), new CancellationException());
      case Future.State.FAILED ->
          new ImmediateFailedStrand<T>(Scope.current(), future.exceptionNow());
      default -> new AsyncStrand<T>(Scope.current(), new FutureTask<T>(future)).start();
    };
  }

  /**
   * Returns a {@link BoundedComposer} that is used to compose one or more candidate {@code Strands}
   * into a single {@code Strand}.
   *
   * @throws IllegalArgumentException if no candidates are provided
   * @throws IllegalStateException if called from outside the task passed to {@link #concurrent}
   */
  public static <T extends @Nullable Object> BoundedComposer<T> compose(Strand<T>... candidates) {
    return new BoundedComposer<T>(Scope.current(), candidates);
  }

  /**
   * Returns a {@link BoundedComposer} that is used to compose an {@link Iterable} of one or more
   * candidate {@code Strands} into a single {@code Strand}.
   *
   * @throws IllegalArgumentException if the iterable is empty
   * @throws IllegalStateException if called from outside the task passed to {@link #concurrent}
   */
  public static <T extends @Nullable Object> BoundedComposer<T> compose(
      Iterable<Strand<T>> candidates) {
    return compose(toArray(candidates));
  }

  /**
   * Returns a {@link BoundedComposer} that is used to compose a {@link Stream} of one or more
   * candidate {@code Strands} into a single {@code Strand}.
   *
   * @throws IllegalArgumentException if the stream is empty
   * @throws IllegalStateException if called from outside the task passed to {@link #concurrent}
   */
  public static <T extends @Nullable Object> BoundedComposer<T> compose(
      Stream<Strand<T>> candidates) {
    return compose(toArray(candidates));
  }

  /**
   * Creates a new scope of execution within the current scope and runs the given {@link Task} in
   * that scope.
   *
   * @throws IllegalStateException if called from outside the task passed to {@link #concurrent}
   * @throws InterruptedException if the current thread was interrupted while running the task
   * @throws X if the task failed with an exception rather than returning a result
   */
  public static <T extends @Nullable Object, X extends Throwable> T scope(Task<T, X> task)
      throws X {
    ExecutionContext context = Scope.current().context();
    return Scope.<T, X>run(context, task);
  }

  /**
   * Returns a builder for creating a {@link Gatherer} that executes tasks concurrently using
   * Strands.
   *
   * <p>Note: The produced {@link Gatherer} cannot be used with parallel streams.
   *
   * <pre>{@code
   * List<Result<Response>> results = requests.stream()
   *     .gather(Strands.gather()
   *         .concurrency(10)
   *         .unordered()
   *         .timeout(Duration.ofSeconds(10))
   *         .map(req -> stub.call(req)))
   *     .toList();
   * }</pre>
   */
  public static FluentGatherer gather() {
    return new FluentGatherer(Scope.current());
  }

  /**
   * Runs the given task in a new virtual thread that may execute concurrently to all other calls to
   * {@link #concurrent}.
   *
   * <p>Within the given {@link Task}, any created {@code Strands} (virtual threads that return
   * results to the caller) will be independent units of execution, but they will be coordinated
   * such that only one Strand (virtual thread) will be running at a time, providing a "single
   * execution" single-execution guarantee. Strands created within different calls to {@link
   * #concurrent} do not have this single-execution guarantee.
   *
   * <p>Calls to {@link #concurrent} can be nested, which creates independent scopes of execution
   * that depend on each other but do not share a single-execution guarantee.
   *
   * <p>{@link #concurrent} can be thought of starting a tree of execution of virtual threads
   * (Strands) that have a single-execution guarantee. Each call to {@link #concurrent} starts a new
   * independent tree.
   *
   * <p>If an exception escapes {@code task}, the returned {@link ListenableFuture} fails with that
   * exception (unwrapping {@link FailedTaskException} if thrown by a child {@link Strand}).
   * Cancelling the returned {@link ListenableFuture} (via either {@code cancel(true)} or {@code
   * cancel(false)}) interrupts the root virtual thread and cancels all child Strands in the scope.
   *
   * <p>Note that an unhandled {@link CancellationException} escaping {@code task} (for example,
   * from awaiting a cancelled child {@link Strand}) is treated as a task failure rather than a
   * cancelled result {@link Future}: the returned {@link ListenableFuture} fails with that
   * exception and is only marked {@link Future#isCancelled() cancelled} when {@link
   * Future#cancel(boolean)} is called on it directly.
   *
   * <p>This method uses an implicit {@link Environment}: if the caller already has one active, it
   * uses that, otherwise it uses the default runtime environment. See {@link Environment} for how
   * that default is resolved.
   *
   * @param task the code to execute in a new virtual thread, starting a new tree of virtual threads
   *     that shares a single-execution guarantee. The task will be executed concurrently to the
   *     caller
   * @return the result of running the task, as a {@link ListenableFuture}, since the task will
   *     execute concurrently to the caller
   * @param <T> the type of the return value of the task
   */
  public static <T extends @Nullable Object> ListenableFuture<T> concurrent(
      Task<T, ? extends Throwable> task) {
    return concurrent(null, task);
  }

  /**
   * Runs the given task in a new virtual thread that may execute concurrently to all other calls to
   * {@link #concurrent}.
   *
   * <p>Within the given task, any created {@code Strands} (virtual threads that return results to
   * the caller) will be independent units of execution, but they will be coordinated such that only
   * one Strand (virtual thread) will be running at a time, providing a "single execution"
   * single-execution guarantee. Strands created within different calls to {@link #concurrent} do
   * not have this single-execution guarantee.
   *
   * <p>Calls to {@link #concurrent} can be nested, which creates independent scopes of execution
   * that depend on each other but do not share a single-execution guarantee.
   *
   * <p>{@link #concurrent} can be thought of starting a tree of execution of virtual threads
   * (Strands) that have a single-execution guarantee. Each call to {@link #concurrent} starts a new
   * independent tree.
   *
   * <p>If an exception escapes {@code task}, the returned {@link ListenableFuture} fails with that
   * exception (unwrapping {@link FailedTaskException} if thrown by a child {@link Strand}).
   * Cancelling the returned {@link ListenableFuture} (via either {@code cancel(true)} or {@code
   * cancel(false)}) interrupts the root virtual thread and cancels all child Strands in the scope.
   *
   * <p>Note that an unhandled {@link CancellationException} escaping {@code task} (for example,
   * from awaiting a cancelled child {@link Strand}) is treated as a task failure rather than a
   * cancelled result {@link Future}: the returned {@link ListenableFuture} fails with that
   * exception and is only marked {@link Future#isCancelled() cancelled} when {@link
   * Future#cancel(boolean)} is called on it directly.
   *
   * <p>This method uses an explicit {@link Environment}, which allows the caller to specify
   * framework behavior for this tree of virtual threads instead of inheriting that behavior from
   * the caller or the runtime environment.
   *
   * @param environment the explicit {@link Environment} to use.
   * @param task the code to execute in a new virtual thread, starting a new tree of virtual threads
   *     that shares a single-execution guarantee. The task will be executed concurrently to the
   *     caller.
   * @return the result of running the task, as a {@link ListenableFuture}, since the task will
   *     execute concurrently to the caller.
   * @param <T> the type of the return value.
   */
  @SuppressWarnings("Interruption") // Intentionally interrupting the virtual thread.
  public static <T extends @Nullable Object> ListenableFuture<T> concurrent(
      @Nullable Environment environment, Task<T, ? extends Throwable> task) {
    VirtualThreadFactory virtualThreadFactory = VirtualThreadFactory.task();

    ExecutionContext context =
        switch (environment) {
          case null ->
              Scope.isBound()
                  ? Scope.current().context().withTaskThreadFactory(virtualThreadFactory)
                  : ExecutionContext.create(virtualThreadFactory, DefaultEnvironment.get());
          default -> ExecutionContext.create(virtualThreadFactory, environment);
        };

    SettableFuture<T> result = SettableFuture.create();
    Runnable runnable =
        () -> {
          try {
            result.set(Scope.run(context, task));
          } catch (Throwable e) {
            if (e instanceof InterruptedException) {
              Thread.currentThread().interrupt(); // Restore the interrupted status
            }
            Throwable cause = e instanceof FailedTaskException x ? requireNonNull(x.getCause()) : e;
            // All unhandled exceptions escaping the root task (including an unhandled
            // CancellationException from awaiting a cancelled child Strand or upstream Future)
            // represent a failure of the graph and are reported via setException(cause), preserving
            // the original exception instance, message, and stack trace. The root ListenableFuture
            // only transitions to isCancelled() == true when Future.cancel(...) is called on it
            // directly by an external caller.
            result.setException(cause);
          }
        };
    if (context.contextPropagationOperator() != null) {
      runnable = context.contextPropagationOperator().apply(runnable);
    }

    // Create a new virtual thread
    // WARNING: this is a potential platform thread boundary, ensure anything crossing the
    // boundary has appropriate memory consistency
    Thread vt = virtualThreadFactory.newVirtualThread(runnable);

    result.addListener(
        () -> {
          if (result.isCancelled()) {
            // Unconditionally interrupt the root virtual thread whenever the returned Future is
            // cancelled (regardless of whether cancel(true) or cancel(false) was called). Because
            // vt is started immediately and virtual thread interruption is Strands' only mechanism
            // for cooperative scope cancellation, skipping interruption on cancel(false) would
            // leave the entire Strands scope and its child virtual threads running in the
            // background as an orphaned tree after result already reported itself as done.
            vt.interrupt();
          }
        },
        directExecutor());
    vt.start();
    return result;
  }

  /** Converts a {@link Stream} of {@link Strand} to an array of {@link Strand}. */
  @SuppressWarnings("unchecked") // Cast is safe.
  private static <T extends @Nullable Object> Strand<T>[] toArray(Stream<Strand<T>> candidates) {
    return candidates.<Strand<T>>toArray(size -> (Strand<T>[]) new Strand<?>[size]);
  }

  /** Converts an {@link Iterable} of {@link Strand} to an array of {@link Strand}. */
  @SuppressWarnings("unchecked") // Cast is safe.
  private static final <T extends @Nullable Object> Strand<T>[] toArray(
      Iterable<Strand<T>> iterable) {
    return (Strand<T>[]) Iterables.toArray(iterable, Strand.class);
  }

  private Strands() {}
}
