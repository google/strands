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

import com.google.async.strands.Gatherers.Pusher;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Gatherer;
import org.jspecify.annotations.Nullable;

/**
 * A {@link Gatherer} implementation that executes transformations concurrently and emits results as
 * soon as they are ready.
 *
 * @param <T> the type of the input elements
 * @param <V> the type of the intermediate results
 * @param <R> the type of the output elements
 */
final class UnorderedGatherer<
    T extends @Nullable Object, V extends @Nullable Object, R extends @Nullable Object> {

  /**
   * Creates a {@link Gatherer} that executes transformations concurrently and emits results as soon
   * as they are ready.
   *
   * @param <T> the type of the input elements
   * @param <V> the type of the intermediate results
   * @param <R> the type of the output elements
   * @param options the options for the gatherer
   * @param mapper the mapper function to apply to each element
   * @param pusher the pusher to use for pushing the results
   */
  static <T extends @Nullable Object, V extends @Nullable Object, R extends @Nullable Object>
      Gatherer<T, ?, R> create(
          FluentGatherer.Options options,
          Transform<? super T, ? extends V, ? extends Throwable> mapper,
          Pusher<V, R> pusher) {
    return Gatherer.ofSequential(
        () -> new UnorderedGatherer<T, V, R>(options, mapper, pusher),
        Gatherer.Integrator.ofGreedy(UnorderedGatherer::integrate),
        (state, downstream) -> state.finish(downstream));
  }

  private final Scope scope;
  private final Duration timeout;
  private final Semaphore concurrency;
  private final Transform<? super T, ? extends V, ? extends Throwable> mapper;
  private final BlockingQueue<Result<V>> results;
  private final Set<Strand<?>> active;
  private final Semaphore bufferCapacity;
  private final Gatherers.Pusher<V, R> pusher;
  private final AtomicInteger pending;

  private UnorderedGatherer(
      FluentGatherer.Options options,
      Transform<? super T, ? extends V, ? extends Throwable> mapper,
      Gatherers.Pusher<V, R> pusher) {
    this.scope = options.scope();
    this.timeout = options.timeout();
    this.concurrency = new Semaphore(options.concurrencyLimit());
    this.mapper = mapper;
    this.results = new LinkedBlockingQueue<>(options.bufferSize());
    this.active = ConcurrentHashMap.newKeySet();
    this.bufferCapacity = new Semaphore(options.bufferSize());
    this.pusher = pusher;
    this.pending = new AtomicInteger(0);
  }

  /**
   * Provides the implementation for a {@link Gatherer.Integrator.Greedy} that performs
   * transformations concurrently and emits results as soon as they are ready.
   *
   * <p>This method will block if the buffer is full, waiting for space to become available.
   *
   * @return false if the downstream is rejecting further elements.
   */
  boolean integrate(T element, Gatherer.Downstream<? super R> downstream) {
    if (!flush(downstream)) {
      return false;
    }

    try {
      bufferCapacity.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      cancelAll();
      return false;
    }

    // Double check if we should still proceed.
    if (!flush(downstream)) {
      bufferCapacity.release();
      return false;
    }

    startTask(element);
    return true;
  }

  private void startTask(T element) {
    pending.incrementAndGet();
    AsyncStrand<V> strand =
        new AsyncStrand<>(
            scope,
            () -> {
              concurrency.acquire();
              try {
                return mapper.apply(element);
              } finally {
                concurrency.release();
              }
            });
    if (!strand.pushCallback(
        () -> {
          results.add(requireNonNull(strand.result()));
          active.remove(strand);
          bufferCapacity.release();
        })) {
      throw new StrandsInternalStateException("Callback not appended on unstarted strand");
    }
    active.add(strand);
    strand.start();

    if (!timeout.isZero()) {
      // If there's a timeout, start a new framework internal thread to handle timing out the
      // strand. We use a framework internal thread here to avoid tasks blocking actual timeout
      // handling. This is safe as this logic doesn't affect data sharing within the scope.
      scope.startFrameworkInternalThread(
          () -> {
            try {
              Thread.sleep(timeout);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt(); // Reset the interrupted status.
            } finally {
              // Note: We can unconditionally attempt this transition because in the case where
              // the thread was joined successfully, this is guaranteed to have no effect.
              strand.timeout();
            }
          });
    }
  }

  /**
   * Flushes the buffer and propagates any remaining results to the downstream.
   *
   * <p>This method will block there are still pending results, waiting for them to complete.
   *
   * @param downstream the downstream to push the results to
   */
  void finish(Gatherer.Downstream<? super R> downstream) {
    while (pending.get() > 0) {
      try {
        Result<V> result = results.take();
        pending.decrementAndGet();
        try {
          if (!pusher.push(result, downstream)) {
            cancelAll();
            return;
          }
        } catch (RuntimeException | Error e) {
          cancelAll();
          throw e;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        cancelAll();
        return;
      }
    }
    var _ = flush(downstream);
  }

  /**
   * Flushes the buffer by pushing results to the downstream.
   *
   * @param downstream the downstream to push the results to
   * @return false if the downstream is rejecting further elements.
   */
  private boolean flush(Gatherer.Downstream<? super R> downstream) {
    Result<V> res;
    while ((res = results.poll()) != null) {
      pending.decrementAndGet();
      try {
        if (!pusher.push(res, downstream)) {
          cancelAll();
          return false;
        }
      } catch (RuntimeException | Error e) {
        cancelAll();
        throw e;
      }
    }
    if (downstream.isRejecting()) {
      cancelAll();
      return false;
    }
    return true;
  }

  /** Cancels all active strands and clears the buffer. */
  private void cancelAll() {
    for (Strand<?> strand : active) {
      strand.cancel();
    }
    active.clear();
  }
}
