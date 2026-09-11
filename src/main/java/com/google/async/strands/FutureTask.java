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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.jspecify.annotations.Nullable;

/** A {@link AsyncStrand.CancellableTask} that wraps a {@link Future} as its task. */
final class FutureTask<T extends @Nullable Object>
    implements AsyncStrand.CancellableTask<T, Throwable> {
  private final Future<T> future;

  FutureTask(Future<T> future) {
    this.future = future;
  }

  @Override
  @SuppressWarnings("Interruption") // Propagating the interruption to the future.
  public T run() throws Throwable {
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); // Restore the interrupted status
      future.cancel(true);
      throw e;
    } catch (ExecutionException e) {
      // Try to unwrap since any exception we throw will be wrapped in a FailedTaskException.
      Throwable cause = e.getCause();
      throw cause != null ? cause : e;
    }
  }

  @Override
  public void cancel() {
    future.cancel(false);
  }
}
