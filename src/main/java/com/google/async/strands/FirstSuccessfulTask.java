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
import static java.util.Objects.requireNonNull;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.Nullable;

/**
 * A {@link AsyncStrand.CancellableTask} that is the evaluates to the result of the first candidate
 * Strand to complete successfully; any remaining candidates are cancelled. If all candidates fail,
 * the failure of the first candidate to fail is thrown with the failures of the other candidates as
 * suppressed exceptions.
 */
final class FirstSuccessfulTask<T extends @Nullable Object>
    implements AsyncStrand.CancellableTask<T, Throwable> {

  private static final VarHandle SUCCESS_HANDLE;
  private static final VarHandle FAILURE_HANDLE;

  static {
    try {
      Lookup lookup = MethodHandles.lookup();
      SUCCESS_HANDLE = lookup.findVarHandle(FirstSuccessfulTask.class, "success", boolean.class);
      FAILURE_HANDLE = lookup.findVarHandle(FirstSuccessfulTask.class, "failure", Throwable.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final Strand<T>[] candidates;
  private volatile boolean success;
  private volatile @Nullable T result;
  private volatile @Nullable Throwable failure;

  FirstSuccessfulTask(Strand<T>... candidates) {
    checkArgument(candidates.length > 0, "At least one candidate must be provided");
    this.candidates = candidates;
  }

  @Override
  public T run() throws Throwable {
    AtomicInteger pendingCount = new AtomicInteger(candidates.length);
    CountDownLatch latch = new CountDownLatch(1);

    process:
    for (Strand<T> candidate : candidates) {
      AbstractStrand<T> strand = (AbstractStrand<T>) candidate;
      @SuppressWarnings({
        "nullness", // compareAndSet accepts null and guarantees failure is non-null.
        "ReferenceEquality" // Reference check to avoid trying to suppress a failure with itself.
      })
      ThreadSafeRunnable onComplete =
          () -> {
            Result<T> result = requireNonNull(strand.result());
            if (result.isOk()) {
              if (SUCCESS_HANDLE.compareAndSet(this, false, true)) {
                this.result = result.value();
                latch.countDown(); // We have a result, release everything.
                return;
              }
            } else {
              Throwable failure = requireNonNull(result.failure());
              if (!FAILURE_HANDLE.compareAndSet(this, null, failure) && this.failure != failure) {
                // First failure was already captured, just add this as suppressed.
                this.failure.addSuppressed(failure);
              }
            }
            if (pendingCount.decrementAndGet() == 0) {
              latch.countDown(); // Mark this candidate as done.
            }
          };
      switch (strand.state()) {
        case SUCCEEDED -> {
          onComplete.run();
          break process;
        }
        case FAILED, CANCELLED, TIMEOUT -> {
          onComplete.run();
          continue;
        }
        default -> {
          if (!strand.pushCallback(onComplete)) {
            // Race condition where the strand completed between the switch statement and now.
            onComplete.run();
          }
        }
      }
    }
    // Wait for one candidate to complete successfully or all to fail.
    try {
      latch.await();
    } catch (InterruptedException e) {
      // The current thread was interrupted (NOT the candidate threads) while waiting. Cancel the
      // candidates since they're no longer needed then propagate.
      Thread.currentThread().interrupt(); // Restore the interrupted status
      cancel();
      throw e;
    }
    if (success) {
      cancel();
      return (T) result;
    }
    throw requireNonNull(failure);
  }

  @Override
  public void cancel() {
    for (Strand<T> candidate : candidates) {
      candidate.cancel();
    }
  }
}
