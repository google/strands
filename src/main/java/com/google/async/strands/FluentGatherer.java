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

import com.google.async.strands.Gatherers.FailFastFlattenPusher;
import com.google.async.strands.Gatherers.FailFastIdentityPusher;
import com.google.async.strands.Gatherers.ResultFlattenPusher;
import com.google.async.strands.Gatherers.ResultIdentityPusher;
import java.time.Duration;
import java.util.stream.Gatherer;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;

/**
 * Fluent builder for creating a {@link Gatherer} that executes transformations concurrently using
 * Strands.
 *
 * <p>Note: The produced {@link Gatherer} cannot be used with parallel streams.
 */
@SuppressWarnings("CanIgnoreReturnValueSuggester") // We never want a return value ignored.
public final class FluentGatherer {

  final Scope scope;
  int bufferSize;
  int concurrencyLimit;
  Duration timeout;
  boolean ordered;

  FluentGatherer(Scope scope) {
    this.scope = scope;
    this.bufferSize = 8;
    this.concurrencyLimit = 8;
    this.timeout = Duration.ZERO;
    this.ordered = true;
  }

  /**
   * Sets the maximum number of elements the gatherer can buffer at a time.
   *
   * <p>A larger buffer size allows for more decoupling between the input and output, but uses more
   * memory. When the buffer is full, the gatherer will block until space is available and will not
   * process any more input elements even if the concurrency limit has not been reached.
   *
   * <p>Note: This cannot be set to a value smaller than the concurrency limit.
   *
   * <p>Defaults to {@code 8}.
   */
  public FluentGatherer buffer(int size) {
    if (size < 1) {
      throw new IllegalArgumentException("'size' must be positive");
    }
    this.bufferSize = size;
    return this;
  }

  /**
   * Sets the maximum number of concurrent Strands to allow (following the single-execution
   * guarantee).
   *
   * <p>A larger concurrency limit allows for more concurrent execution, but may result in more
   * context switching and less efficient execution, especially if the transformations are not I/O
   * bound. When the concurrency limit is reached, the gatherer will block until a Strand completes
   * and space is available and will not process any more input elements even if the buffer is not
   * full.
   *
   * <p>Note: This cannot be set to a value larger than the buffer size.
   *
   * <p>Defaults to {@code 8}.
   */
  public FluentGatherer concurrency(int limit) {
    if (limit < 1) {
      throw new IllegalArgumentException("'limit' must be positive");
    }
    this.concurrencyLimit = limit;
    return this;
  }

  /**
   * Configures the gatherer to emit results as soon as they are ready, regardless of the input
   * encounter order.
   *
   * <p>If ordering of the results is not important, using this option can be more efficient.
   *
   * <p>By default, results are emitted in the order of the input elements.
   */
  public FluentGatherer unordered() {
    this.ordered = false;
    return this;
  }

  /**
   * Sets the timeout for each individual transformation.
   *
   * <p>If the timeout is reached, the transformation will fail with a {@link TimeoutException}.
   *
   * <p>Defaults to {@code Duration.ZERO}, which means no timeout.
   */
  public FluentGatherer timeout(Duration timeout) {
    if (timeout.isNegative()) {
      throw new IllegalArgumentException("'timeout' must be non-negative");
    }
    this.timeout = timeout;
    return this;
  }

  /**
   * Returns a {@link Gatherer} that maps each input element to a {@link Result} using the
   * configured parameters.
   *
   * @param mapper the async {@link Transform} to apply to each input element
   * @throws IllegalStateException if the concurrency limit is greater than the buffer size
   */
  public <T extends @Nullable Object, R extends @Nullable Object> Gatherer<T, ?, Result<R>> map(
      Transform<? super T, ? extends R, ? extends Throwable> mapper) {
    Options options = new Options(scope, bufferSize, concurrencyLimit, timeout, ordered);
    return options.ordered()
        ? OrderedGatherer.create(options, mapper, ResultIdentityPusher.instance())
        : UnorderedGatherer.create(options, mapper, ResultIdentityPusher.instance());
  }

  /**
   * Returns a {@link Gatherer} that maps each input element to a {@link Stream} of {@link Result}
   * instances and flattens them into the output using the configured parameters.
   *
   * @param mapper the async {@link Transform} to apply to each input element
   * @throws IllegalStateException if the concurrency limit is greater than the buffer size
   */
  public <T extends @Nullable Object, R extends @Nullable Object> Gatherer<T, ?, Result<R>> flatMap(
      Transform<? super T, ? extends Stream<? extends R>, ? extends Throwable> mapper) {
    Options options = new Options(scope, bufferSize, concurrencyLimit, timeout, ordered);
    return options.ordered()
        ? OrderedGatherer.create(options, mapper, ResultFlattenPusher.instance())
        : UnorderedGatherer.create(options, mapper, ResultFlattenPusher.instance());
  }

  /**
   * Returns a {@link Gatherer} that maps each input element to its result, or aborts the stream if
   * any mapping fails.
   *
   * @param mapper the async {@link Transform} to apply to each input element
   * @throws IllegalStateException if the concurrency limit is greater than the buffer size
   */
  public <T extends @Nullable Object, R extends @Nullable Object> Gatherer<T, ?, R> mapFailFast(
      Transform<? super T, ? extends R, ? extends Throwable> mapper) {
    FluentGatherer.Options options =
        new FluentGatherer.Options(scope, bufferSize, concurrencyLimit, timeout, ordered);
    return options.ordered()
        ? OrderedGatherer.create(options, mapper, FailFastIdentityPusher.instance())
        : UnorderedGatherer.create(options, mapper, FailFastIdentityPusher.instance());
  }

  /**
   * Returns a {@link Gatherer} that maps each input element to a {@link Stream} and flattens them
   * into the output, or aborts the stream if any mapping fails.
   *
   * @param mapper the async {@link Transform} to apply to each input element
   * @throws IllegalStateException if the concurrency limit is greater than the buffer size
   */
  public <T extends @Nullable Object, R extends @Nullable Object> Gatherer<T, ?, R> flatMapFailFast(
      Transform<? super T, ? extends Stream<? extends R>, ? extends Throwable> mapper) {
    FluentGatherer.Options options =
        new FluentGatherer.Options(scope, bufferSize, concurrencyLimit, timeout, ordered);
    return options.ordered()
        ? OrderedGatherer.create(options, mapper, FailFastFlattenPusher.instance())
        : UnorderedGatherer.create(options, mapper, FailFastFlattenPusher.instance());
  }

  /** Configuration options for the gatherer. */
  static record Options(
      Scope scope, int bufferSize, int concurrencyLimit, Duration timeout, boolean ordered) {
    Options {
      if (bufferSize < 1) {
        throw new IllegalArgumentException("'bufferSize' must be positive");
      }
      if (concurrencyLimit < 1) {
        throw new IllegalArgumentException("'concurrencyLimit' must be positive");
      }
      if (timeout.isNegative()) {
        throw new IllegalArgumentException("'timeout' must be non-negative");
      }
      if (concurrencyLimit > bufferSize) {
        throw new IllegalStateException(
            String.format(
                "concurrency limit (%d) cannot be greater than buffer size (%d)",
                concurrencyLimit, bufferSize));
      }
    }
  }
}
