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

/**
 * Exception thrown by {@link Strand#await()} when the outcome is an exception rather than a result.
 */
public final class FailedTaskException extends RuntimeException {
  /**
   * Wraps the provided {@code cause} in a {@link FailedTaskException} if it is not already a {@code
   * FailedTaskException}.
   */
  public static FailedTaskException wrap(Throwable cause) {
    if (cause instanceof FailedTaskException x) {
      return x;
    }
    return new FailedTaskException(cause);
  }

  public FailedTaskException(Throwable cause) {
    super(cause);
  }
}
