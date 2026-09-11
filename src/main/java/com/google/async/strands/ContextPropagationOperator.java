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

import com.google.errorprone.annotations.ThreadSafe;
import java.util.function.UnaryOperator;

/**
 * An operator that wraps a {@link Runnable} to run it with some additional context.
 *
 * <p>When a new thread is being allocated, {@link ContextPropagationOperator#apply(Runnable)} will
 * be called on the current thread and the returned {@link Runnable} will be executed on a new
 * virtual thread. Therefore, it's expected that any capture work happens in {@code apply}. This can
 * be used to propagate {@link java.lang.ThreadLocal} values or custom {@link java.lang.ScopedValue}
 * bindings to these new threads (standard Java 21+ / 25 does not automatically inherit arbitrary
 * {@code ScopedValue} bindings across non-StructuredTaskScope thread boundaries).
 *
 * <p>Strands <i>may</i> automatically propagate {@code ScopedValue} bindings in the future if a
 * suitable mechanism can be found. If this happens, manually propagating them via this interface
 * should still work correctly, though it may be redundant.
 */
@ThreadSafe
@FunctionalInterface
public interface ContextPropagationOperator extends UnaryOperator<Runnable> {
  /**
   * Returns a context operator that always returns its input argument (i.e. it does not propagate
   * any context).
   */
  static ContextPropagationOperator identity() {
    return r -> r;
  }

  /**
   * Returns a {@link Runnable} that wraps the given {@link Runnable} to run it with some additional
   * context.
   *
   * <p>When a new thread is being allocated, this method will be called on the current thread and
   * the returned {@link Runnable} will be executed on a new virtual thread. Therefore, it's
   * expected that any capture work happens in {@code apply}. This can be used to propagate {@link
   * java.lang.ThreadLocal} values to these new threads.
   */
  @Override
  Runnable apply(Runnable runnable);
}
