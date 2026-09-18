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
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.google.errorprone.annotations.ThreadSafe;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

/**
 * A lightweight grouping of virtual threads for a {@link Scope}.
 *
 * <p>Tracks active threads using a concurrent set of {@link Thread} instances and coordinates
 * thread completion using atomic counters and {@link LockSupport}.
 */
@ThreadSafe
final class ScopeThreadFlock implements AutoCloseable {

  private static final VarHandle THREAD_COUNT_HANDLE;
  private static final VarHandle PERMIT_HANDLE;

  static {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      THREAD_COUNT_HANDLE = lookup.findVarHandle(ScopeThreadFlock.class, "threadCount", int.class);
      PERMIT_HANDLE = lookup.findVarHandle(ScopeThreadFlock.class, "permit", boolean.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final Thread owner = Thread.currentThread();
  private final String name;

  // Set of currently registered Interruptible entities in this flock
  @ThreadSafe.Suppress // ConcurrentHashMap.newKeySet() is thread safe.
  private final Set<Thread> registeredThreads = ConcurrentHashMap.newKeySet();

  // Count of active threads in this flock
  @ThreadSafe.Suppress // VarHandle guarded.
  private volatile int threadCount;

  @ThreadSafe.Suppress // State flag.
  private volatile boolean shutdown;
  @ThreadSafe.Suppress // State flag.
  private volatile boolean closed;
  @ThreadSafe.Suppress // VarHandle guarded via PERMIT_HANDLE.
  private volatile boolean permit;

  ScopeThreadFlock(String name) {
    this.name = name;
  }

  /** Returns the owner thread of this flock. */
  Thread owner() {
    return owner;
  }

  /** Returns the name of this flock. */
  String name() {
    return name;
  }

  /** Returns the number of active threads in this flock. */
  int threadCount() {
    return threadCount;
  }

  /** Registers a thread with this flock. */
  void register(Thread thread) {
    checkState(!shutdown && !closed, "Flock is shutdown or closed");
    THREAD_COUNT_HANDLE.getAndAdd(this, 1);
    registeredThreads.add(thread);
    if (shutdown || closed) {
      registeredThreads.remove(thread);
      decrementThreadCount();
      throw new IllegalStateException("Flock is shutdown or closed");
    }
  }

  /** Unregisters a thread from this flock. */
  void unregister(Thread thread) {
    if (registeredThreads.remove(thread)) {
      decrementThreadCount();
    }
  }

  private void decrementThreadCount() {
    int count = (int) THREAD_COUNT_HANDLE.getAndAdd(this, -1) - 1;
    if (count == 0) {
      LockSupport.unpark(owner);
    }
  }

  /** Waits for all threads in this flock to finish executing. */
  public boolean awaitAll() throws InterruptedException {
    ensureOwner();

    if (getAndSetPermit(false)) {
      return threadCount == 0;
    }

    while (threadCount > 0 && !permit) {
      LockSupport.park();
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
    }
    clearPermit();
    return threadCount == 0;
  }

  /** Waits up to a timeout for all threads in this flock to finish executing. */
  public boolean awaitAll(Duration timeout) throws InterruptedException, TimeoutException {
    checkNotNull(timeout);
    checkArgument(!timeout.isNegative(), "timeout must be non-negative: %s", timeout);
    ensureOwner();

    if (getAndSetPermit(false)) {
      return threadCount == 0;
    }

    long startNanos = System.nanoTime();
    long nanos = NANOSECONDS.convert(timeout);
    long remainingNanos = nanos;
    while (threadCount > 0 && remainingNanos > 0 && !permit) {
      LockSupport.parkNanos(remainingNanos);
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      remainingNanos = nanos - (System.nanoTime() - startNanos);
    }

    boolean done = threadCount == 0;
    if (!done && remainingNanos <= 0 && !permit) {
      throw new TimeoutException();
    } else {
      clearPermit();
      return done;
    }
  }

  /** Causes any active call to {@link #awaitAll()} to return immediately. */
  public void wakeup() {
    if (!getAndSetPermit(true) && Thread.currentThread() != owner) {
      LockSupport.unpark(owner);
    }
  }

  /** Transitions this flock to the shutdown state, preventing new threads from starting. */
  public void shutdown() {
    shutdown = true;
  }

  @Override
  public void close() {
    ensureOwner();
    if (closed) {
      return;
    }

    if (!shutdown) {
      shutdown = true;
    }

    boolean interrupted = false;
    try {
      while (threadCount > 0) {
        LockSupport.park();
        if (Thread.interrupted()) {
          interrupted = true;
        }
      }
    } finally {
      closed = true;
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Interrupts all active threads currently registered in this flock. */
  @SuppressWarnings("Interruption") // Intentionally interrupting the threads.
  public void interruptAll() {
    for (Thread thread : registeredThreads) {
      thread.interrupt();
    }
  }

  public boolean isShutdown() {
    return shutdown;
  }

  public boolean isClosed() {
    return closed;
  }

  private void ensureOwner() {
    checkState(Thread.currentThread() == owner, "Current thread is not the flock owner");
  }

  private void clearPermit() {
    var _ = getAndSetPermit(false);
  }

  private boolean getAndSetPermit(boolean newValue) {
    return (boolean) PERMIT_HANDLE.getAndSet(this, newValue);
  }
}
