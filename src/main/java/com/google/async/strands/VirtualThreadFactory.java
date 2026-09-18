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

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.concurrent.ThreadFactory;

/**
 * A thread-safe factory for creating virtual threads that is shared by all Strands calls within a
 * single {@link Strands#concurrent} call.
 */
@ThreadSafe
final class VirtualThreadFactory {

  /**
   * Returns a {@link VirtualThreadFactory} that can be used to create virtual threads that are
   * executed by the framework and unbound from any single execution guarantee.
   *
   * <p>Threads provided by this factory <b>must not</b> ever be used to execute tasks submitted to
   * Strands, rather only internal support tasks.
   */
  static VirtualThreadFactory framework() {
    return framework;
  }

  private static final VirtualThreadFactory framework =
      new VirtualThreadFactory(VirtualThreadSupport.frameworkThreadFactory());

  /**
   * Returns a {@link VirtualThreadFactory} that can be used to create virtual threads that execute
   * tasks for Strands.
   */
  static VirtualThreadFactory task() {
    return new VirtualThreadFactory(VirtualThreadSupport.taskThreadFactory());
  }

  @GuardedBy("this")
  private final ThreadFactory threadFactory;

  private VirtualThreadFactory(ThreadFactory threadFactory) {
    this.threadFactory = threadFactory;
  }

  /**
   * Constructs a new <i>unstarted</i> virtual thread to run the given runnable.
   *
   * @throws IllegalStateException if the returned thread is not actually virtual or if it is
   *     already started
   */
  final synchronized Thread newVirtualThread(Runnable runnable) {
    Thread thread = requireNonNull(threadFactory.newThread(runnable));
    checkState(
        thread.isVirtual(),
        "VirtualThreadFactory attempted to provide a non-virtual (platform) thread");
    checkState(
        !thread.isAlive(),
        "VirtualThreadFactory attempted to provide a thread that is already started");
    return thread;
  }
}
