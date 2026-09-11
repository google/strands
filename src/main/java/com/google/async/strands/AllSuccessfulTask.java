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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;

/**
 * A {@link AsyncStrand.CancellableTask} that collects the results of one or more Strands into a
 * {@link Stream} that maintains the order of the elements. If an element strand fails, this task
 * will fail with the first failure encountered from any of the elements and cancel all remaining
 * strands.
 */
final class AllSuccessfulTask<T extends @Nullable Object>
    implements AsyncStrand.CancellableTask<Stream<T>, Throwable> {

  private static final VarHandle FAILURE_HANDLE;

  static {
    try {
      Lookup lookup = MethodHandles.lookup();
      FAILURE_HANDLE = lookup.findVarHandle(AllSuccessfulTask.class, "failure", Throwable.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final Strand<T>[] elements;
  private volatile @Nullable Throwable failure;

  AllSuccessfulTask(Strand<T>... elements) {
    if (elements.length == 0) {
      throw new IllegalArgumentException("At least one element must be provided");
    }
    this.elements = elements;
  }

  @Override
  public Stream<T> run() throws Throwable {
    AtomicInteger pendingCount = new AtomicInteger(elements.length);
    CountDownLatch done = new CountDownLatch(1);
    @SuppressWarnings("unchecked") // Array of generic type.
    T[] results = (T[]) new Object[elements.length];

    process:
    for (int i = 0; i < elements.length; i++) {
      int index = i;
      AbstractStrand<T> strand = (AbstractStrand<T>) elements[i];
      ThreadSafeRunnable onComplete =
          () -> {
            Result<T> result = requireNonNull(strand.result());
            if (result.isOk()) {
              results[index] = result.value();
              if (pendingCount.decrementAndGet() == 0) {
                done.countDown(); // Mark this candidate as done, and if it is the last, release.
              }
            } else {
              @SuppressWarnings("nullness") // compareAndSet accepts null without issue.
              var unused = FAILURE_HANDLE.compareAndSet(this, null, result.failure());
              done.countDown();
            }
          };
      switch (strand.state()) {
        case SUCCEEDED -> {
          onComplete.run();
          continue;
        }
        case FAILED, CANCELLED, TIMEOUT -> {
          onComplete.run();
          break process;
        }
        default -> {
          if (!strand.pushCallback(onComplete)) {
            // Race condition where the strand completed between the switch statement and now.
            onComplete.run();
          }
        }
      }
    }
    // Wait for one candidate to fail or all to complete successfully.
    try {
      done.await();
    } catch (InterruptedException e) {
      // The current thread - not the element threads - was interrupted while waiting. Cancel the
      // elements since they're no longer needed then propagate.
      Thread.currentThread().interrupt(); // Restore the interrupted status
      cancel();
      throw e;
    }
    if (failure == null) {
      return stream(results);
    }
    cancel();
    throw requireNonNull(failure);
  }

  @Override
  public void cancel() {
    for (Strand<T> element : elements) {
      element.cancel();
    }
  }
}
