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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Duration;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.jspecify.annotations.Nullable;

/**
 * A union type of either a value or failure (exception) that represents the result of a {@link
 * Strand}.
 */
public sealed interface Result<T extends @Nullable Object> permits Result.Ok, Result.Failed {

  /** Returns {@code true} if this result represents a successful {@link Strand}. */
  boolean isOk();

  /**
   * Returns {@code true} if this result represents a failed (including cancelled and timeouts)
   * {@link Strand}.
   */
  boolean isFailed();

  /**
   * Returns the value a {@link Strand} completed successfully with.
   *
   * @throws IllegalStateException if this result represents a failure.
   */
  T value();

  /**
   * Returns the exception a {@link Strand} failed with.
   *
   * @throws IllegalStateException if this result represents a success.
   */
  Throwable failure();

  /**
   * Returns the value of this result or throws the exception that describes the failure of this
   * result.
   *
   * @throws FailedTaskException that wraps {@link #failure()} if this result is failed
   */
  default T get() {
    if (isOk()) {
      return value();
    }
    throw FailedTaskException.wrap(failure());
  }

  /**
   * If the result is not failed, performs the given {@code action} with the result's value,
   * otherwise does nothing.
   */
  default void ifOk(Consumer<? super T> action) {
    if (isOk()) {
      action.accept(value());
    }
  }

  /**
   * If the result is failed, performs the given {@code action} with the result's failure, otherwise
   * does nothing.
   *
   * <p>The {@link Consumer} should not throw the provided exception, {@link #orElseThrow} should be
   * used instead.
   */
  default void ifFailed(Consumer<? super Throwable> action) {
    if (isFailed()) {
      action.accept(failure());
    }
  }

  /**
   * Returns a {@code Result} whose value is either this result's value or {@code defaultValue} if
   * this result is failed.
   */
  default <R extends T> Result<T> orElse(R defaultValue) {
    return !isOk() ? ofValue(defaultValue) : this;
  }

  /**
   * Returns a {@code Result} whose value is either this result's value or {@code defaultValue} if
   * this result is failed with a {@link Throwable} that is an instance of a specific {@link Class}.
   *
   * <p>If this result is failed with a failure of a different {@code Class}, this result is
   * returned unchanged.
   */
  default <R extends T> Result<T> orElse(Class<? extends Throwable> failureClass, R defaultValue) {
    return !isOk() && failureClass.isInstance(failure()) ? ofValue(defaultValue) : this;
  }

  /**
   * Returns a {@code Result} whose value is either this result's value or {@code defaultValue} if
   * this result is failed with a {@link Throwable} that matches a {@link Predicate}.
   *
   * <p>If this result is failed and the {@code Predicate} does not match the failure, this result
   * is returned unchanged.
   */
  default <R extends T> Result<T> orElse(
      Predicate<? super Throwable> failurePredicate, R defaultValue) {
    return !isOk() && failurePredicate.test(failure()) ? ofValue(defaultValue) : this;
  }

  /**
   * Returns a {@code Result} whose value is either this result's value or the value provided by a
   * {@link Task} if this result is failed.
   */
  default <X extends Throwable> Result<T> orElseGet(Task<? extends T, X> valueTask) throws X {
    return !isOk() ? ofValue(valueTask.run()) : this;
  }

  /**
   * Returns a {@code Result} whose value is either this result's value or the value provided by a
   * {@link Task} if this result is failed with a {@link Throwable} that is an instance of a
   * specific {@link Class}.
   *
   * <p>If this result is failed with a failure of a different {@code Class}, this result is
   * returned unchanged.
   */
  default <X extends Throwable> Result<T> orElseGet(
      Class<? extends Throwable> failureClass, Task<? extends T, X> valueTask) throws X {
    return !isOk() && failureClass.isInstance(failure()) ? ofValue(valueTask.run()) : this;
  }

  /**
   * Returns a {@code Result} whose value is either this result's value or the value provided by a
   * {@link Task} if this result is failed with a {@link Throwable} that matches a {@link
   * Predicate}.
   *
   * <p>If this result is failed and the {@code Predicate} does not match the failure, this result
   * is returned unchanged.
   */
  default <X extends Throwable> Result<T> orElseGet(
      Predicate<? super Throwable> failurePredicate, Task<? extends T, X> valueTask) throws X {
    return !isOk() && failurePredicate.test(failure()) ? ofValue(valueTask.run()) : this;
  }

  /**
   * Returns a {@code Result} whose value is either this result's value or the {@code Result}
   * returned by {@link Strand#awaitResult()} after running a {@link Task} with {@link
   * Strands#async(Task)}.
   *
   * <p>This is a convenience for performing an async operation as part of a {@link
   * #orElseGet(Task)} call:
   *
   * <pre>{@code
   * result.orElseAsync(task)
   * }</pre>
   *
   * <p>Is equivalent to:
   *
   * <pre>{@code
   * result.orElseGet(() -> async(task).await())
   * }</pre>
   *
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete
   */
  default Result<T> orElseAsync(Task<T, ? extends Throwable> task) throws InterruptedException {
    return orElseAsync(task, Duration.ZERO);
  }

  /**
   * Returns a {@code Result} whose value is either this result's value or the {@code Result}
   * returned by {@link Strand#awaitResult(Duration)} after running an {@link Task} with {@link
   * Strands#async(Task)}.
   *
   * <p>This is a convenience for performing an async operation as part of a {@link
   * #orElseGet(Task)} call:
   *
   * <pre>{@code
   * result.orElseAsync(task, timeout)
   * }</pre>
   *
   * <p>Is equivalent to:
   *
   * <pre>{@code
   * result.orElseGet(() -> async(task).await(timeout))
   * }</pre>
   *
   * @param timeout the maximum duration to wait for the Strand's task to complete, {@link
   *     Duration#ZERO} indicates there is no timeout and behaves the same as calling {@link
   *     #orElseAsync(Task)}
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete
   */
  default Result<T> orElseAsync(Task<T, ? extends Throwable> task, Duration timeout)
      throws InterruptedException {
    return !isOk() ? Strands.async(task).awaitResult(timeout) : this;
  }

  /**
   * Returns a {@code Result} whose value is either this result's value or the {@code Result}
   * returned by {@link Strand#awaitResult()} after running a {@link Future} with {@link
   * Strands#async(Future)}.
   *
   * <p>This is a convenience for performing an async operation as part of a {@link
   * #orElseGet(Task)} call:
   *
   * <pre>{@code
   * result.orElseAsync(future)
   * }</pre>
   *
   * <p>Is equivalent to:
   *
   * <pre>{@code
   * result.orElseGet(() -> async(future).await())
   * }</pre>
   *
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete
   */
  default Result<T> orElseAsync(Future<T> future) throws InterruptedException {
    return orElseAsync(future, Duration.ZERO);
  }

  /**
   * Returns a {@code Result} whose value is either this result's value or the {@code Result}
   * returned by {@link Strand#awaitResult(Duration)} after running an {@link Future} with {@link
   * Strands#async(Future)}.
   *
   * <p>This is a convenience for performing an async operation as part of a {@link
   * #orElseGet(Task)} call:
   *
   * <pre>{@code
   * result.orElseAsync(future, timeout)
   * }</pre>
   *
   * <p>Is equivalent to:
   *
   * <pre>{@code
   * result.orElseGet(() -> async(future).await(timeout))
   * }</pre>
   *
   * @param timeout the maximum duration to wait for the Strand's task to complete, {@link
   *     Duration#ZERO} indicates there is no timeout and behaves the same as calling {@link
   *     #orElseAsync(Future)}
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete
   */
  default Result<T> orElseAsync(Future<T> future, Duration timeout) throws InterruptedException {
    return !isOk() ? Strands.async(future).awaitResult(timeout) : this;
  }

  /**
   * Returns a {@code Result} whose value is either this result's value or the {@code Result}
   * returned by {@link Strand#awaitResult()} after running an {@link Task} with {@link
   * Strands#async(Task)} if this result is failed with a {@link Throwable} that is an instance of a
   * specific {@link Class}.
   *
   * <p>This is a convenience for performing an async operation as part of a {@link
   * #orElseGet(Class, Task)} call:
   *
   * <pre>{@code
   * result.orElseAsync(SomeException.class, task)
   * }</pre>
   *
   * <p>Is equivalent to:
   *
   * <pre>{@code
   * result.orElseGet(SomeException.class, () -> async(task).await())
   * }</pre>
   *
   * <p>If this result is failed with a failure of a different {@code Class}, this result is
   * returned unchanged.
   *
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete
   */
  default Result<T> orElseAsync(
      Class<? extends Throwable> failureClass, Task<T, ? extends Throwable> task)
      throws InterruptedException {
    return orElseAsync(failureClass, task, Duration.ZERO);
  }

  /**
   * Returns a {@code Result} whose value is either this result's value or the {@code Result}
   * returned by {@link Strand#awaitResult(Duration)} after running an {@link Task} with {@link
   * Strands#async(Task)} if this result is failed with a {@link Throwable} that is an instance of a
   * specific {@link Class}.
   *
   * <p>This is a convenience for performing an async operation as part of a {@link
   * #orElseGet(Class, Task)} call:
   *
   * <pre>{@code
   * result.orElseAsync(SomeException.class, task, timeout)
   * }</pre>
   *
   * <p>Is equivalent to:
   *
   * <pre>{@code
   * result.orElseGet(SomeException.class, () -> async(task).await(timeout))
   * }</pre>
   *
   * <p>If this result is failed with a failure of a different {@code Class}, this result is
   * returned unchanged.
   *
   * @param timeout the maximum duration to wait for the Strand's task to complete, {@link
   *     Duration#ZERO} indicates there is no timeout and behaves the same as calling {@link
   *     #orElseAsync(Class, Task)}
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete
   */
  default Result<T> orElseAsync(
      Class<? extends Throwable> failureClass, Task<T, ? extends Throwable> task, Duration timeout)
      throws InterruptedException {
    return !isOk() && failureClass.isInstance(failure())
        ? Strands.async(task).awaitResult(timeout)
        : this;
  }

  /**
   * Returns a {@code Result} whose value is either this result's value or the {@code Result}
   * returned by {@link Strand#awaitResult()} after running an {@link Future} with {@link
   * Strands#async(Future)} if this result is failed with a {@link Throwable} that is an instance of
   * a specific {@link Class}.
   *
   * <p>This is a convenience for performing an async operation as part of a {@link
   * #orElseGet(Class, Task)} call:
   *
   * <pre>{@code
   * result.orElseAsync(SomeException.class, future)
   * }</pre>
   *
   * <p>Is equivalent to:
   *
   * <pre>{@code
   * result.orElseGet(SomeException.class, () -> async(future).await())
   * }</pre>
   *
   * <p>If this result is failed with a failure of a different {@code Class}, this result is
   * returned unchanged.
   *
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete
   */
  default Result<T> orElseAsync(Class<? extends Throwable> failureClass, Future<T> future)
      throws InterruptedException {
    return orElseAsync(failureClass, future, Duration.ZERO);
  }

  /**
   * Returns a {@code Result} whose value is either this result's value or the {@code Result}
   * returned by {@link Strand#awaitResult(Duration)} after running an {@link Future} with {@link
   * Strands#async(Future)} if this result is failed with a {@link Throwable} that is an instance of
   * a specific {@link Class}.
   *
   * <p>This is a convenience for performing an async operation as part of a {@link
   * #orElseGet(Class, Task)} call:
   *
   * <pre>{@code
   * result.orElseAsync(SomeException.class, future, timeout)
   * }</pre>
   *
   * <p>Is equivalent to:
   *
   * <pre>{@code
   * result.orElseGet(SomeException.class, () -> async(future).await(timeout))
   * }</pre>
   *
   * <p>If this result is failed with a failure of a different {@code Class}, this result is
   * returned unchanged.
   *
   * @param timeout the maximum duration to wait for the Strand's task to complete, {@link
   *     Duration#ZERO} indicates there is no timeout and behaves the same as calling {@link
   *     #orElseAsync(Class, Future)}
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete
   */
  default Result<T> orElseAsync(
      Class<? extends Throwable> failureClass, Future<T> future, Duration timeout)
      throws InterruptedException {
    return !isOk() && failureClass.isInstance(failure())
        ? Strands.async(future).awaitResult(timeout)
        : this;
  }

  /**
   * Returns a {@code Result} whose value is either this result's value or the {@code Result}
   * returned by {@link Strand#awaitResult()} after running an {@link Task} with {@link
   * Strands#async(Task)} if this result is failed with a {@link Throwable} that matches a {@link
   * Predicate}.
   *
   * <p>This is a convenience for performing an async operation as part of a {@link
   * #orElseGet(Predicate, Task)} call:
   *
   * <pre>{@code
   * result.orElseAsync(predicate, task)
   * }</pre>
   *
   * <p>Is equivalent to:
   *
   * <pre>{@code
   * result.orElseGet(predicate, () -> async(task).await())
   * }</pre>
   *
   * <p>If this result is failed and the {@code Predicate} does not match the failure, this result
   * is returned unchanged.
   *
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete
   */
  default Result<T> orElseAsync(
      Predicate<? super Throwable> failurePredicate, Task<T, ? extends Throwable> task)
      throws InterruptedException {
    return orElseAsync(failurePredicate, task, Duration.ZERO);
  }

  /**
   * Returns a {@code Result} whose value is either this result's value or the {@code Result}
   * returned by {@link Strand#awaitResult(Duration)} after running an {@link Task} with {@link
   * Strands#async(Task)} if this result is failed with a {@link Throwable} that matches a {@link
   * Predicate}.
   *
   * <p>This is a convenience for performing an async operation as part of a {@link
   * #orElseGet(Predicate, Task)} call:
   *
   * <pre>{@code
   * result.orElseAsync(predicate, task, timeout)
   * }</pre>
   *
   * <p>Is equivalent to:
   *
   * <pre>{@code
   * result.orElseGet(predicate, () -> async(task).await(timeout))
   * }</pre>
   *
   * <p>If this result is failed and the {@code Predicate} does not match the failure, this result
   * is returned unchanged.
   *
   * @param timeout the maximum duration to wait for the Strand's task to complete, {@link
   *     Duration#ZERO} indicates there is no timeout and behaves the same as calling {@link
   *     #orElseAsync(Predicate, Task)}
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete
   */
  default Result<T> orElseAsync(
      Predicate<? super Throwable> failurePredicate,
      Task<T, ? extends Throwable> task,
      Duration timeout)
      throws InterruptedException {
    return !isOk() && failurePredicate.test(failure())
        ? Strands.async(task).awaitResult(timeout)
        : this;
  }

  /**
   * Returns a {@code Result} whose value is either this result's value or the {@code Result}
   * returned by {@link Strand#awaitResult()} after running an {@link Future} with {@link
   * Strands#async(Future)} if this result is failed with a {@link Throwable} that matches a {@link
   * Predicate}.
   *
   * <p>This is a convenience for performing an async operation as part of a {@link
   * #orElseGet(Predicate, Task)} call:
   *
   * <pre>{@code
   * result.orElseAsync(predicate, future)
   * }</pre>
   *
   * <p>Is equivalent to:
   *
   * <pre>{@code
   * result.orElseGet(predicate, () -> async(future).await())
   * }</pre>
   *
   * <p>If this result is failed and the {@code Predicate} does not match the failure, this result
   * is returned unchanged.
   *
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete
   */
  default Result<T> orElseAsync(Predicate<? super Throwable> failurePredicate, Future<T> future)
      throws InterruptedException {
    return orElseAsync(failurePredicate, future, Duration.ZERO);
  }

  /**
   * Returns a {@code Result} whose value is either this result's value or the {@code Result}
   * returned by {@link Strand#awaitResult(Duration)} after running an {@link Future} with {@link
   * Strands#async(Future)} if this result is failed with a {@link Throwable} that matches a {@link
   * Predicate}.
   *
   * <p>This is a convenience for performing an async operation as part of a {@link
   * #orElseGet(Predicate, Task)} call:
   *
   * <pre>{@code
   * result.orElseAsync(predicate, future, timeout)
   * }</pre>
   *
   * <p>Is equivalent to:
   *
   * <pre>{@code
   * result.orElseGet(predicate, () -> async(future).await(timeout))
   * }</pre>
   *
   * <p>If this result is failed and the {@code Predicate} does not match the failure, this result
   * is returned unchanged.
   *
   * @param timeout the maximum duration to wait for the Strand's task to complete, {@link
   *     Duration#ZERO} indicates there is no timeout and behaves the same as calling {@link
   *     #orElseAsync(Predicate, Future)}
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete
   */
  default Result<T> orElseAsync(
      Predicate<? super Throwable> failurePredicate, Future<T> future, Duration timeout)
      throws InterruptedException {
    return !isOk() && failurePredicate.test(failure())
        ? Strands.async(future).awaitResult(timeout)
        : this;
  }

  /**
   * Returns a {@code Result} whose value is the result of applying a {@link Transform} to this
   * result's value.
   *
   * <p>If this result is failed, this result is returned unchanged, albeit cast to the result type.
   */
  default <R extends @Nullable Object, X extends Throwable> Result<R> map(
      Transform<? super T, ? extends R, X> valueTransform) throws X {
    return isOk() ? ofValue(valueTransform.apply(value())) : ofFailure(failure());
  }

  /**
   * Returns a {@code Result} returned by {@link Strand#awaitResult()} whose value is the result of
   * applying a {@link Transform} to this result's value run with {@link Strands#async(Task)}.
   *
   * <p>This is a convenience for performing an async operation as part of a {@link #map(Transform)}
   * call:
   *
   * <pre>{@code
   * result.mapAsync(transform)
   * }</pre>
   *
   * <p>Is equivalent to:
   *
   * <pre>{@code
   * result.map(v -> async(() -> transform.apply(v)).await())
   * }</pre>
   *
   * <p>If this result is failed, this result is returned unchanged, albeit cast to the result type.
   *
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete
   */
  default <R extends @Nullable Object> Result<R> mapAsync(
      Transform<? super T, ? extends R, ? extends Throwable> valueTransform)
      throws InterruptedException {
    return isOk()
        ? Strands.<R>async(() -> valueTransform.apply(value())).awaitResult()
        : ofFailure(failure());
  }

  /**
   * Returns a {@code Result} returned by {@link Strand#awaitResult(Duration)} whose value is the
   * result of applying a {@link Transform} to this result's value run with {@link
   * Strands#async(Task)}.
   *
   * <p>This is a convenience for performing an async operation as part of a {@link #map(Transform)}
   * call:
   *
   * <pre>{@code
   * result.mapAsync(transform, timeout)
   * }</pre>
   *
   * <p>Is equivalent to:
   *
   * <pre>{@code
   * result.map(v -> async(() -> transform.apply(v)).await(timeout))
   * }</pre>
   *
   * <p>If this result is failed, this result is returned unchanged, albeit cast to the result type.
   *
   * @param timeout the maximum duration to wait for the Strand's task to complete, {@link
   *     Duration#ZERO} indicates there is no timeout and behaves the same as calling {@link
   *     #mapAsync(Transform)}
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete
   */
  default <R extends @Nullable Object> Result<R> mapAsync(
      Transform<? super T, ? extends R, ? extends Throwable> valueTransform, Duration timeout)
      throws InterruptedException {
    return isOk()
        ? Strands.<R>async(() -> valueTransform.apply(value())).awaitResult(timeout)
        : ofFailure(failure());
  }

  /**
   * Returns a {@code Result} whose value is the result of applying a {@link Transform} that returns
   * a {@code Result} to this result's value.
   *
   * <p>If this result is failed, this result is returned unchanged.
   */
  @SuppressWarnings("unchecked")
  default <R extends @Nullable Object, X extends Throwable> Result<R> flatMap(
      Transform<? super T, Result<R>, X> valueTransform) throws X {
    return isOk() ? valueTransform.apply(value()) : (Result<R>) this;
  }

  /**
   * Returns a {@code Result} whose value is the result of applying a {@link Transform} that returns
   * a {@code Result} to this result's value run with {@link Strands#async(Task)}.
   *
   * <p>If this result is failed, this result is returned unchanged.
   *
   * @throws InterruptedException if the current thread was interrupted while waiting for the Strand
   *     to complete
   */
  @SuppressWarnings("unchecked")
  default <R extends @Nullable Object> Result<R> flatMapAsync(
      Transform<? super T, Result<R>, ? extends Throwable> valueTransform)
      throws InterruptedException {
    if (!isOk()) {
      return (Result<R>) this;
    }
    Result<Result<R>> nested =
        Strands.<Result<R>>async(() -> valueTransform.apply(value())).awaitResult();
    return nested.isOk() ? nested.value() : (Result<R>) ofFailure(nested.failure());
  }

  /**
   * Returns a {@code Result} whose value is the result of applying a {@link Transform} to this
   * result's value or {@code defaultValue} if this result is failed.
   */
  default <R extends @Nullable Object, D extends R, X extends Throwable> Result<R> mapOrDefault(
      Transform<? super T, ? extends R, X> valueTransform, D defaultValue) throws X {
    return isOk() ? ofValue(valueTransform.apply(value())) : ofValue(defaultValue);
  }

  /**
   * Returns a {@code Result} whose value is the result of applying a {@link Transform} to this
   * result's value or the value returned by a {@link Task} if this result is failed.
   */
  default <R extends @Nullable Object, X extends Throwable, Y extends Throwable>
      Result<R> mapOrElse(
          Transform<? super T, ? extends R, X> valueTransform, Task<? extends R, Y> valueTask)
          throws X, Y {
    return isOk() ? ofValue(valueTransform.apply(value())) : ofValue(valueTask.run());
  }

  /**
   * Returns this result or throws the result of applying a {@link Function} to this result's
   * failure.
   *
   * <p>If this result is not failed, this result is returned unchanged.
   */
  @CanIgnoreReturnValue
  default <X extends Throwable> Result<T> orElseThrow(
      Function<? super Throwable, X> failureTransform) throws X {
    if (!isOk()) {
      throw failureTransform.apply(failure());
    }
    return this;
  }

  /**
   * Returns this result or throws the result of applying a {@link Function} to this result's
   * failure if the failure is an instance of a specific {@link Class}.
   */
  @CanIgnoreReturnValue
  default <X extends Throwable> Result<T> orElseThrow(
      Class<? extends Throwable> failureClass, Function<? super Throwable, X> failureTransform)
      throws X {
    if (isOk() || !failureClass.isInstance(failure())) {
      return this;
    }
    throw failureTransform.apply(failure());
  }

  /**
   * Returns this result or throws the result of applying a {@link Function} to this result's
   * failure if the failure matches a {@link Predicate}.
   */
  @CanIgnoreReturnValue
  default <X extends Throwable> Result<T> orElseThrow(
      Predicate<? super Throwable> failurePredicate,
      Function<? super Throwable, X> failureTransform)
      throws X {
    if (isOk() || !failurePredicate.test(failure())) {
      return this;
    }
    throw failureTransform.apply(failure());
  }

  /** Returns a {@link Result} that is successful with the given value. */
  static <T extends @Nullable Object> Result<T> ofValue(T value) {
    return new Ok<>(value);
  }

  /** Returns a {@link Result} that is failed with the given {@link Throwable}. */
  static <T extends @Nullable Object> Result<T> ofFailure(Throwable failure) {
    return new Failed<T>(failure);
  }

  /** A {@link Result} that is successful with a value. */
  static record Ok<T extends @Nullable Object>(T value) implements Result<T> {
    @Override
    public boolean isOk() {
      return true;
    }

    @Override
    public boolean isFailed() {
      return false;
    }

    @Override
    public T value() {
      return value;
    }

    @Override
    public Throwable failure() {
      throw new IllegalStateException("Result is not failed");
    }
  }

  /** A {@link Result} that is failed with a {@link Throwable}. */
  static record Failed<T extends @Nullable Object>(Throwable failure) implements Result<T> {
    @Override
    public boolean isOk() {
      return false;
    }

    @Override
    public boolean isFailed() {
      return true;
    }

    @Override
    public T value() {
      throw new IllegalStateException("Result is not successful", failure);
    }

    @Override
    public Throwable failure() {
      return failure;
    }
  }
}
