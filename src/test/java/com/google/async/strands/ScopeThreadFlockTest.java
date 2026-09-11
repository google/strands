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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ScopeThreadFlockTest {

  private ScopeThreadFlock flock;

  @Before
  public void setUp() {
    flock = new ScopeThreadFlock("testFlock");
  }

  @After
  public void tearDown() {
    flock.close();
  }

  @Test
  public void ownerConfinement_nonOwnerCallingOwnerMethods_throwsIllegalStateException()
      throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> awaitAllFuture =
          executor.submit(() -> assertThrows(IllegalStateException.class, () -> flock.awaitAll()));
      var _ = awaitAllFuture.get();

      Future<?> awaitTimedFuture =
          executor.submit(
              () ->
                  assertThrows(
                      IllegalStateException.class, () -> flock.awaitAll(Duration.ofMillis(100))));
      var _ = awaitTimedFuture.get();

      Future<?> closeFuture =
          executor.submit(() -> assertThrows(IllegalStateException.class, () -> flock.close()));
      var _ = closeFuture.get();
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void singleThread_registerAndUnregister_adjustsCountAndUnblocksAwait() throws Exception {
    Thread thread = Thread.ofVirtual().unstarted(() -> {});

    flock.register(thread);
    assertThat(flock.threadCount()).isEqualTo(1);

    flock.unregister(thread);
    assertThat(flock.threadCount()).isEqualTo(0);
    assertThat(flock.awaitAll()).isTrue();
  }

  @Test
  public void wakeup_beforeAwaitAll_setsPermitAndReturnsFalse() throws Exception {
    Thread thread = Thread.ofVirtual().unstarted(() -> {});
    CountDownLatch latch = new CountDownLatch(1);

    flock.register(thread);
    flock.wakeup(); // Set permit before awaitAll

    boolean allFinished = flock.awaitAll();
    assertThat(allFinished)
        .isFalse(); // Returns false because threads are unfinished and wakeup was called

    // Second awaitAll should block until exit because permit was cleared by first
    // awaitAll
    var _ =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    latch.await();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } finally {
                    flock.unregister(thread);
                  }
                });

    latch.countDown();
    assertThat(flock.awaitAll()).isTrue();
  }

  @Test
  public void wakeup_whileBlockedInAwaitAll_unparksOwner() throws Exception {
    Thread thread = Thread.ofVirtual().unstarted(() -> {});

    flock.register(thread);

    CountDownLatch enteringAwaitLatch = new CountDownLatch(1);

    // Concurrently call wakeup from another thread after owner signals entering awaitAll
    var _ =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    enteringAwaitLatch.await();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } finally {
                    flock.wakeup();
                  }
                });

    enteringAwaitLatch.countDown();
    boolean allFinished = flock.awaitAll(); // Will unpark when wakeup is called
    assertThat(allFinished).isFalse();

    flock.unregister(thread);
  }

  @Test
  public void awaitAllWithTimeout_negativeDuration_throwsIllegalArgumentException()
      throws Exception {
    assertThrows(IllegalArgumentException.class, () -> flock.awaitAll(Duration.ofMillis(-50)));
  }

  @Test
  public void awaitAllWithTimeout_expires_throwsTimeoutException() {
    Thread thread = Thread.ofVirtual().unstarted(() -> {});

    flock.register(thread);

    assertThrows(TimeoutException.class, () -> flock.awaitAll(Duration.ofMillis(50)));

    flock.unregister(thread);
  }

  @Test
  public void unregister_removesThreadFromRegisteredSet_noMemoryLeak() throws Exception {
    Thread thread1 = Thread.ofVirtual().unstarted(() -> {});

    flock.register(thread1);
    assertThat(flock.threadCount()).isEqualTo(1);

    flock.unregister(thread1);
    assertThat(flock.threadCount()).isEqualTo(0);

    // Verify interruptAll after exit executes cleanly without touching finished thread
    flock.interruptAll();
  }

  @Test
  public void interruptAll_iteratesRegisteredThreads_interruptsAllThreads() throws Exception {
    int threadCount = 10;
    CountDownLatch readyLatch = new CountDownLatch(threadCount);
    CountDownLatch interruptLatch = new CountDownLatch(threadCount);
    List<Thread> threads = new ArrayList<>();

    for (int i = 0; i < threadCount; i++) {
      Thread thread =
          Thread.ofVirtual()
              .start(
                  () -> {
                    readyLatch.countDown();
                    try {
                      Thread.sleep(60000);
                    } catch (InterruptedException e) {
                      interruptLatch.countDown();
                    }
                  });
      threads.add(thread);
      flock.register(thread);
    }

    readyLatch.await();
    flock.interruptAll();
    interruptLatch.await();

    assertThat(interruptLatch.getCount()).isEqualTo(0);

    for (Thread thread : threads) {
      flock.unregister(thread);
    }
  }

  @Test
  public void concurrentRegisterAndUnregister_highStress_maintainsExactThreadCount()
      throws Exception {
    ScopeThreadFlock flock = new ScopeThreadFlock("stressFlock");
    int numThreads = 20;
    int opsPerThread = 500;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);
    AtomicInteger totalStarted = new AtomicInteger(0);
    AtomicReference<Throwable> errorRef = new AtomicReference<>();

    for (int t = 0; t < numThreads; t++) {
      var _ =
          executor.submit(
              () -> {
                try {
                  startLatch.await();
                  for (int i = 0; i < opsPerThread; i++) {
                    Thread thread = Thread.ofVirtual().unstarted(() -> {});
                    flock.register(thread);
                    totalStarted.incrementAndGet();
                    flock.unregister(thread);
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  errorRef.compareAndSet(null, e);
                } catch (Throwable tError) {
                  errorRef.compareAndSet(null, tError);
                } finally {
                  doneLatch.countDown();
                }
              });
    }

    startLatch.countDown();
    doneLatch.await();
    executor.shutdown();

    assertThat(errorRef.get()).isNull();
    assertThat(totalStarted.get()).isEqualTo(numThreads * opsPerThread);
    assertThat(flock.threadCount()).isEqualTo(0);
    assertThat(flock.awaitAll()).isTrue();
  }

  @Test
  public void register_afterClose_throwsIllegalStateException() {
    flock.close();

    Thread thread = Thread.ofVirtual().unstarted(() -> {});
    assertThrows(IllegalStateException.class, () -> flock.register(thread));
  }

  @Test
  public void close_isIdempotent() {
    flock.close();

    flock.close(); // Second call should be no-op without error

    assertThat(flock.isClosed()).isTrue();
  }

  @Test
  public void rawVirtualThread_interruptBeforeStart_immediatelyInterruptsBlockingCall()
      throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean threwInterrupted = new AtomicBoolean(false);
    Thread t =
        Thread.ofVirtual()
            .unstarted(
                () -> {
                  try {
                    Thread.sleep(60000);
                  } catch (InterruptedException e) {
                    threwInterrupted.set(true);
                    latch.countDown();
                  }
                });
    t.interrupt();
    t.start();
    latch.await();
    assertThat(threwInterrupted.get()).isTrue();
  }

  @Test
  public void close_ownerInterruptedWhileWaitingForThreads_preservesInterruptStatusAndWaits()
      throws Exception {
    CountDownLatch readyLatch = new CountDownLatch(1);
    CountDownLatch enteringCloseLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(1);
    AtomicBoolean ownerInterrupted = new AtomicBoolean(false);
    AtomicReference<ScopeThreadFlock> flockRef = new AtomicReference<>();

    Thread thread = Thread.ofVirtual().unstarted(() -> {});

    Thread ownerThread =
        Thread.ofPlatform()
            .start(
                () -> {
                  ScopeThreadFlock flock = new ScopeThreadFlock("interruptedCloseFlock");
                  flockRef.set(flock);
                  flock.register(thread);
                  readyLatch.countDown();

                  enteringCloseLatch.countDown();
                  // close() will park waiting for thread (threadCount == 1)
                  flock.close();

                  ownerInterrupted.set(Thread.interrupted());
                  doneLatch.countDown();
                });

    readyLatch.await();
    ScopeThreadFlock flock = flockRef.get();

    enteringCloseLatch.await();
    // Interrupt the owner thread while waiting inside flock.close()
    ownerThread.interrupt();

    // Verify owner thread is still waiting in close() and has not exited prematurely
    assertThat(doneLatch.getCount()).isEqualTo(1);

    // Complete thread exit to unpark owner thread
    flock.unregister(thread);

    doneLatch.await();
    assertThat(doneLatch.getCount()).isEqualTo(0);
    assertThat(ownerInterrupted.get()).isTrue();
    assertThat(flock.isClosed()).isTrue();
  }

  @Test
  public void awaitAllWithTimeout_permitSetViaWakeup_returnsFalseWithoutTimeoutException()
      throws Exception {
    Thread thread = Thread.ofVirtual().unstarted(() -> {});

    flock.register(thread);
    flock.wakeup(); // Pre-set permit

    // Since permit was set, awaitAll with timeout should return false immediately
    // without throwing TimeoutException, even though threadCount > 0
    boolean result = flock.awaitAll(Duration.ofMillis(100));
    assertThat(result).isFalse();

    flock.unregister(thread);
  }

  @Test
  public void interruptAll_concurrentWithPushThread_interruptsAllThreadsWithoutNPE()
      throws Exception {
    int numThreads = 50;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch readyLatch = new CountDownLatch(numThreads);
    CountDownLatch interruptedLatch = new CountDownLatch(numThreads);
    List<Thread> threads = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      Thread thread =
          Thread.ofVirtual()
              .start(
                  () -> {
                    readyLatch.countDown();
                    try {
                      startLatch.await();
                      Thread.sleep(60000);
                    } catch (InterruptedException e) {
                      interruptedLatch.countDown();
                    }
                  });
      threads.add(thread);
    }

    // Concurrently register threads into flock while owner calls interruptAll
    var _ =
        Thread.ofVirtual()
            .start(
                () -> {
                  for (Thread t : threads) {
                    flock.register(t);
                  }
                  startLatch.countDown();
                });

    readyLatch.await();
    // Call interruptAll concurrently while threads might still be getting registered
    flock.interruptAll();

    // Ensure all threads are interrupted even if interruptAll was called before pusher
    // registered them all
    startLatch.await();
    for (Thread t : threads) {
      t.interrupt();
    }

    interruptedLatch.await();
    assertThat(interruptedLatch.getCount()).isEqualTo(0);

    for (Thread t : threads) {
      flock.unregister(t);
    }
  }

  @Test
  public void register_afterShutdown_throwsIllegalStateException() {
    Thread thread = Thread.ofVirtual().unstarted(() -> {});

    flock.shutdown();
    assertThrows(IllegalStateException.class, () -> flock.register(thread));
  }

  @Test
  public void awaitAll_interrupted_throwsInterruptedExceptionAndClearsStatus() throws Exception {
    CountDownLatch readyLatch = new CountDownLatch(1);
    CountDownLatch enteringAwaitLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(1);
    AtomicBoolean wasInterrupted = new AtomicBoolean(false);
    AtomicBoolean statusCleared = new AtomicBoolean(false);

    Thread thread = Thread.ofVirtual().unstarted(() -> {});

    Thread ownerThread =
        Thread.ofPlatform()
            .start(
                () -> {
                  ScopeThreadFlock flock = new ScopeThreadFlock("interruptedAwaitFlock");
                  flock.register(thread);
                  readyLatch.countDown();

                  enteringAwaitLatch.countDown();
                  try {
                    var _ = flock.awaitAll();
                  } catch (InterruptedException e) {
                    wasInterrupted.set(true);
                    statusCleared.set(!Thread.currentThread().isInterrupted());
                  } finally {
                    flock.unregister(thread);
                    flock.close();
                    doneLatch.countDown();
                  }
                });

    readyLatch.await();
    enteringAwaitLatch.await();

    ownerThread.interrupt();
    doneLatch.await();

    assertThat(wasInterrupted.get()).isTrue();
    assertThat(statusCleared.get()).isTrue();
  }

  @Test
  public void awaitAllWithTimeout_interrupted_throwsInterruptedExceptionAndClearsStatus()
      throws Exception {
    CountDownLatch readyLatch = new CountDownLatch(1);
    CountDownLatch enteringAwaitLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(1);
    AtomicBoolean wasInterrupted = new AtomicBoolean(false);
    AtomicBoolean statusCleared = new AtomicBoolean(false);

    Thread thread = Thread.ofVirtual().unstarted(() -> {});

    Thread ownerThread =
        Thread.ofPlatform()
            .start(
                () -> {
                  ScopeThreadFlock flock = new ScopeThreadFlock("interruptedAwaitTimedFlock");
                  flock.register(thread);
                  readyLatch.countDown();

                  enteringAwaitLatch.countDown();
                  try {
                    var _ = flock.awaitAll(Duration.ofMinutes(1));
                  } catch (InterruptedException e) {
                    wasInterrupted.set(true);
                    statusCleared.set(!Thread.currentThread().isInterrupted());
                  } catch (TimeoutException e) {
                    // Unexpected in this test
                  } finally {
                    flock.unregister(thread);
                    flock.close();
                    doneLatch.countDown();
                  }
                });

    readyLatch.await();
    enteringAwaitLatch.await();

    ownerThread.interrupt();
    doneLatch.await();

    assertThat(wasInterrupted.get()).isTrue();
    assertThat(statusCleared.get()).isTrue();
  }
}
