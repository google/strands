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

import org.jspecify.annotations.Nullable;

/**
 * The root {@link Strand} implementation that includes expanded functionality for concrete
 * implementations.
 *
 * <p>All {@code Strands} are expected to extend this class.
 */
abstract sealed class AbstractStrand<T extends @Nullable Object> implements Strand<T>
    permits AsyncStrand, ImmediateSuccessfulStrand, ImmediateFailedStrand {

  /**
   * Returns the {@link Result} of this Strand or {@code null} if the Strand has not yet completed.
   *
   * <p>This is an internal construct used by other Strands primitives (e.g. {@link
   * FirstCompleteStrand}) to retrieve the result value without directly calling {@link
   * Strand#await()}.
   */
  protected abstract @Nullable Result<T> result();

  /** Returns the {@link Scope} that this Strand belongs to. */
  protected abstract Scope scope();

  /**
   * Atomically pushes a callback onto the Strand's callback stack to be run when the Strand
   * completes. Returns {@code true} if the callback was pushed successfully, or {@code false} if
   * the Strand is already complete.
   *
   * <p>If the Strand is already complete, the callback will <b>not</b> be run, but {@link
   * #result()} is guaranteed to return a non-null value.
   *
   * <p>This is an internal construct used by other Strands primitives (e.g. {@link
   * FirstCompleteStrand}) to avoid creating a separate virtual thread for each callback on every
   * Race call.
   *
   * <p>Callbacks must never throw exceptions and run on the Strand's thread.
   */
  protected abstract boolean pushCallback(ThreadSafeRunnable callback);
}
