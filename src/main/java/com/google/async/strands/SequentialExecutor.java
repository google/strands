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

import static java.lang.System.identityHashCode;

import com.google.common.flogger.GoogleLogger;
import com.google.errorprone.annotations.ThreadSafe;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Executor ensuring that all Runnables submitted are executed in order, using the provided
 * Executor, and sequentially such that no two will ever be running at the same time.
 *
 * <p>Tasks submitted to {@link #execute(Runnable)} are executed in FIFO order.
 *
 * <p>The execution of tasks is done by one thread as long as there are tasks left in the queue.
 * When a task is {@linkplain Thread#interrupt interrupted}, execution of subsequent tasks
 * continues.
 *
 * <p>{@code RuntimeException}s thrown by tasks are simply logged and the executor keeps going. If
 * an {@code Error} is thrown, the error will propagate and execution will stop until it is
 * restarted by a call to {@link #execute}.
 *
 * @implNote This is a slightly more efficient version of {@link
 *     com.google.common.util.concurrent.SequentialExecutor} because the callers are all internal
 *     and we can make some assumptions that avoid defensive allocations.
 */
@ThreadSafe
final class SequentialExecutor implements Executor, Runnable {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  @ThreadSafe.Suppress
  private final ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue<>();

  private final AtomicInteger wip = new AtomicInteger();
  private final Executor delegate;

  SequentialExecutor(Executor delegate) {
    this.delegate = delegate;
  }

  /**
   * Adds a task to the queue and makes sure a worker thread is running.
   *
   * <p>If this method throws, e.g. a {@code RejectedExecutionException} from the delegate executor,
   * execution of tasks will stop until a call to this method is made.
   */
  @Override
  @SuppressWarnings("PreferPreconditions") // Minimizing dependencies.
  public void execute(Runnable task) {
    if (task == null) {
      throw new IllegalArgumentException("task must not be null");
    }
    queue.add(task);
    if (wip.getAndIncrement() == 0) {
      try {
        delegate.execute(this);
      } catch (Throwable t) {
        queue.remove(task);
        wip.getAndSet(0);
        throw t;
      }
    }
  }

  @Override
  @SuppressWarnings("CatchingUnchecked") // Guard against sneaky checked exceptions.
  public void run() {
    boolean interruptedDuringTask = false;
    try {
      int missed = 1;
      do {
        Runnable task;
        while ((task = queue.poll()) != null) {
          // Remove the interrupt bit before each task. The interrupt is for the "current task"
          // when it is sent, so subsequent tasks in the queue should not be caused to be
          // interrupted by a previous one in the queue being interrupted.
          interruptedDuringTask |= Thread.interrupted();
          try {
            task.run();
          } catch (Throwable e) {
            logger.atSevere().withCause(e).log("Exception while executing runnable %s", task);
            if (e instanceof InterruptedException) {
              interruptedDuringTask = true;
            }
          } finally {
            task = null;
          }
        }
        missed = wip.addAndGet(-missed);
      } while (missed != 0);
    } finally {
      // Ensure that if the thread was interrupted at all while processing the task queue, it
      // is returned to the delegate Executor interrupted so that it may handle the interruption
      // if it likes.
      if (interruptedDuringTask) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public String toString() {
    return "SequentialExecutor@" + identityHashCode(this) + "{" + delegate + "}";
  }
}
