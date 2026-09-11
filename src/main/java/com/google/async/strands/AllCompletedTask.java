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

import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;

/**
 * A {@link AsyncStrand.CancellableTask} that collects the results of one or more Strands into a
 * {@link Stream} of {@link Result} instances that maintains the order of the elements. It waits for
 * all elements to complete (either successfully or with a failure) only cancelling them if the
 * current thread is interrupted while waiting.
 */
final class AllCompletedTask<T extends @Nullable Object>
    implements AsyncStrand.CancellableTask<Stream<Result<T>>, InterruptedException> {

  private final Strand<T>[] elements;

  AllCompletedTask(Strand<T>... elements) {
    if (elements.length == 0) {
      throw new IllegalArgumentException("At least one element must be provided");
    }
    this.elements = elements;
  }

  @Override
  public Stream<Result<T>> run() throws InterruptedException {
    CountDownLatch done = new CountDownLatch(elements.length);
    @SuppressWarnings("unchecked") // Array of generic type.
    Result<T>[] results = (Result<T>[]) new Result<?>[elements.length];

    for (int i = 0; i < elements.length; i++) {
      int index = i;
      AbstractStrand<T> strand = (AbstractStrand<T>) elements[i];
      ThreadSafeRunnable onComplete =
          () -> {
            results[index] = requireNonNull(strand.result());
            done.countDown();
          };
      if (!strand.pushCallback(onComplete)) {
        onComplete.run();
      }
    }
    // Wait for all candidates to complete.
    try {
      done.await();
    } catch (InterruptedException e) {
      // The current thread - not the element threads - was interrupted while waiting. Cancel the
      // elements since they're no longer needed then propagate.
      Thread.currentThread().interrupt(); // Restore the interrupted status
      cancel();
      throw e;
    }
    return stream(results);
  }

  @Override
  public void cancel() {
    for (Strand<T> element : elements) {
      element.cancel();
    }
  }
}
