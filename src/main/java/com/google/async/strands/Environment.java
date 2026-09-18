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
import java.util.Optional;

/**
 * Controls Strands framework behavior.
 *
 * <p>This can be thought of as defining an "instance" of the framework within a JVM; if you want
 * different parts of an application to have different overall Strands behavior, use different
 * instances of this interface for the appropriate scopes of your application.
 *
 * <p>{@link Strands#concurrent} is the point of control for choosing which environment to run
 * Strands with; everything within that graph will use the provided environment.
 *
 * <p>If {@link Strands#concurrent} is not provided an explicit environment (the common use case),
 * it uses the default runtime environment, resolved once per JVM from the {@link
 * EnvironmentProvider}s published to Java's {@link java.util.ServiceLoader}. This lets an
 * application, or a library it depends on, customize Strands globally without call sites having to
 * be aware of the configuration. See {@link EnvironmentProvider} for how one is chosen, and for
 * what happens when the classpath does not settle the question.
 *
 * <p>Note that an {@code Environment} is never discovered directly; it is always reached through an
 * {@link EnvironmentProvider}.
 */
@ThreadSafe
public interface Environment {

  /**
   * Returns the {@link EventListener} that should receive monitoring events.
   *
   * <p>Note: This function will be called every time {@link Strands#concurrent} is called, so this
   * should avoid performing expensive operations or allocating a new {@code EventListener} if
   * possible.
   */
  default EventListener eventListener() {
    return EventListener.EMPTY;
  }

  /**
   * Returns the {@link TimingMode} used for measuring event durations in this environment.
   *
   * <p>Defaults to {@link TimingMode#OFF}.
   */
  default TimingMode timingMode() {
    return TimingMode.OFF;
  }

  /**
   * Returns an optional {@link ContextPropagationOperator} that wraps a {@link Runnable} to run it
   * with some additional context.
   *
   * <p>When a new thread is being allocated, {@link ContextPropagationOperator#apply(Runnable)}
   * will be called on the current thread and the returned {@link Runnable} will be executed on the
   * new thread. Therefore, it's expected that any capture work happens in {@code apply}. This can
   * be used to propagate {@link java.lang.ThreadLocal} values to these new threads.
   *
   * <p>Note: This function is called once per {@link Strands#concurrent} call, when the scope's
   * configuration is resolved; the returned operator is then reused for every thread that scope
   * starts.
   */
  default Optional<ContextPropagationOperator> contextPropagationOperator() {
    return Optional.empty();
  }
}
