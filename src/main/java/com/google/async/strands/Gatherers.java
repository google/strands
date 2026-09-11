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

import java.util.stream.Gatherer;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;

/** Internal support for {@link Gatherer} implementations. */
final class Gatherers {

  /** Strategy for pushing results of type V to a downstream expecting Result<R>. */
  @FunctionalInterface
  interface Pusher<T extends @Nullable Object, R extends @Nullable Object> {
    /**
     * Pushes the result to the downstream.
     *
     * @return false if the downstream is rejecting further elements.
     */
    boolean push(Result<T> result, Gatherer.Downstream<? super R> downstream);
  }

  /** {@link Pusher} that pushes the input {@link Result} instances directly to the downstream. */
  static class ResultIdentityPusher<T extends @Nullable Object> implements Pusher<T, Result<T>> {

    private static final ResultIdentityPusher<?> RESULT_IDENTITY_PUSHER =
        new ResultIdentityPusher<>();

    /**
     * Returns a {@link Pusher} that pushes the input {@link Result} instances result directly to
     * the downstream.
     */
    @SuppressWarnings("unchecked") // Cast is safe.
    static <T extends @Nullable Object> Pusher<T, Result<T>> instance() {
      return (Pusher<T, Result<T>>) RESULT_IDENTITY_PUSHER;
    }

    @Override
    public boolean push(Result<T> result, Gatherer.Downstream<? super Result<T>> downstream) {
      return downstream.push(result);
    }
  }

  /**
   * {@link Pusher} that flattens a stream of input {@link Result} instances and pushes them
   * individually to the downstream.
   */
  static class ResultFlattenPusher<T extends @Nullable Object>
      implements Pusher<Stream<? extends T>, Result<T>> {

    private static final ResultFlattenPusher<?> RESULT_FLATTEN_PUSHER = new ResultFlattenPusher<>();

    /**
     * Returns a {@link Pusher} that flattens a stream of input {@link Result} instances and pushes
     * them individually to the downstream.
     */
    @SuppressWarnings("unchecked") // Cast is safe.
    static <T extends @Nullable Object> Pusher<Stream<? extends T>, Result<T>> instance() {
      return (Pusher<Stream<? extends T>, Result<T>>) RESULT_FLATTEN_PUSHER;
    }

    @Override
    public boolean push(
        Result<Stream<? extends T>> result, Gatherer.Downstream<? super Result<T>> downstream) {
      if (!result.isOk()) {
        return downstream.push(Result.ofFailure(result.failure()));
      }
      try (Stream<? extends T> stream = result.value()) {
        if (stream == null) {
          return true;
        }
        var iterator = stream.iterator();
        while (iterator.hasNext()) {
          if (!downstream.push(Result.ofValue(iterator.next()))) {
            return false;
          }
        }
        return true;
      }
    }
  }

  /**
   * {@link Pusher} that pushes the input result values directly to the downstream or throws a
   * {@link FailedTaskException} if any result is not ok.
   */
  static class FailFastIdentityPusher<T extends @Nullable Object> implements Pusher<T, T> {

    private static final FailFastIdentityPusher<?> FAIL_FAST_IDENTITY_PUSHER =
        new FailFastIdentityPusher<>();

    /**
     * Returns a {@link Pusher} that pushes the input result values directly to the downstream or
     * throws a {@link FailedTaskException} if any result is not ok.
     */
    @SuppressWarnings("unchecked") // Cast is safe.
    static <T extends @Nullable Object> Pusher<T, T> instance() {
      return (Pusher<T, T>) FAIL_FAST_IDENTITY_PUSHER;
    }

    @Override
    public boolean push(Result<T> result, Gatherer.Downstream<? super T> downstream) {
      if (!result.isOk()) {
        throw FailedTaskException.wrap(result.failure());
      }
      return downstream.push(result.value());
    }
  }

  /**
   * {@link Pusher} that flattens a stream of input result values and pushes them individually to
   * the downstream or throws a {@link FailedTaskException} if any result is not ok.
   */
  static class FailFastFlattenPusher<T extends @Nullable Object>
      implements Pusher<Stream<? extends T>, T> {

    private static final FailFastFlattenPusher<?> FAIL_FAST_FLATTEN_PUSHER =
        new FailFastFlattenPusher<>();

    /**
     * Returns a {@link Pusher} that flattens a stream of input result values and pushes them
     * individually to the downstream or throws a {@link FailedTaskException} if any result is not
     * ok.
     */
    @SuppressWarnings("unchecked") // Cast is safe.
    static <T extends @Nullable Object> Pusher<Stream<? extends T>, T> instance() {
      return (Pusher<Stream<? extends T>, T>) FAIL_FAST_FLATTEN_PUSHER;
    }

    @Override
    public boolean push(
        Result<Stream<? extends T>> result, Gatherer.Downstream<? super T> downstream) {
      if (!result.isOk()) {
        throw FailedTaskException.wrap(result.failure());
      }
      try (Stream<? extends T> stream = result.value()) {
        if (stream == null) {
          return true;
        }
        var iterator = stream.iterator();
        while (iterator.hasNext()) {
          if (!downstream.push(iterator.next())) {
            return false;
          }
        }
        return true;
      }
    }
  }

  private Gatherers() {}
}
