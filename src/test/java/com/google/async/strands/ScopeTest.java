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

import static com.google.async.strands.Strands.async;
import static com.google.async.strands.Strands.scope;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.async.strands.testing.StrandsRule;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ScopeTest {

  private final StrandsRule strands = StrandsRule.create();

  @Test
  public void nestedScopes_currentScopeBinding_correctlyScoped() {
    var _ =
        strands.run(
            () -> {
              Scope outerScope = Scope.current();
              assertThat(Scope.current()).isSameInstanceAs(outerScope);

              var unusedScopeResult =
                  scope(
                      () -> {
                        Scope innerScope = Scope.current();
                        assertThat(innerScope).isNotSameInstanceAs(outerScope);

                        Strand<Integer> innerStrand = async(() -> 1);
                        assertThat(((AsyncStrand<Integer>) innerStrand).scope())
                            .isSameInstanceAs(innerScope);
                        return innerStrand.await();
                      });

              assertThat(Scope.current()).isSameInstanceAs(outerScope);
              return null;
            });
  }

  @Test
  public void scopeCurrent_outsideConcurrentScope_throwsIllegalStateException() {
    assertThrows(IllegalStateException.class, Scope::current);
  }

  @Test
  public void startFrameworkThread_executesTaskWithCurrentScopeBound() throws Exception {
    var _ =
        strands.run(
            () -> {
              Scope scope = Scope.current();
              CountDownLatch latch = new CountDownLatch(1);
              AtomicBoolean scopeBound = new AtomicBoolean(false);

              scope.startFrameworkInternalThread(
                  () -> {
                    if (Scope.current() == scope) {
                      scopeBound.set(true);
                    }
                    latch.countDown();
                  });

              latch.await();
              assertThat(scopeBound.get()).isTrue();
              return null;
            });
  }

  @Test
  public void contextPropagationOperator_wrapsTaskExecutionOnVirtualThread() {
    AtomicBoolean operatorCalled = new AtomicBoolean(false);

    Environment customEnv =
        new Environment() {
          @Override
          public Optional<ContextPropagationOperator> contextPropagationOperator() {
            return Optional.of(
                runnable -> {
                  operatorCalled.set(true);
                  return runnable;
                });
          }
        };

    StrandsRule customTester = StrandsRule.create(customEnv);

    var _ =
        customTester.run(
            () -> {
              Strand<Integer> strand = async(() -> 42);
              assertThat(strand.await()).isEqualTo(42);
              return null;
            });

    assertThat(operatorCalled.get()).isTrue();
  }

  @Test
  public void highVolumeSpawning_nestedScopes_allCompleteSuccessfully() {
    int count =
        strands.run(
            () -> {
              AtomicInteger sum = new AtomicInteger(0);
              int numSubScopes = 20;
              int strandsPerScope = 50;
              for (int i = 0; i < numSubScopes; i++) {
                var unusedScope =
                    scope(
                        () -> {
                          List<Strand<Integer>> strandsList = new ArrayList<>();
                          for (int j = 0; j < strandsPerScope; j++) {
                            final int val = j;
                            strandsList.add(async(() -> val));
                          }
                          for (Strand<Integer> strand : strandsList) {
                            sum.addAndGet(strand.await());
                          }
                          return null;
                        });
              }
              return sum.get();
            });

    // 20 * (0 + 1 + ... + 49) = 20 * (49 * 50 / 2) = 20 * 1225 = 24500
    assertThat(count).isEqualTo(24500);
  }

  @Test
  public void scopeOwnerInterrupted_childStrandsCancelledAndInterrupted() {
    AtomicInteger startedCount = new AtomicInteger(0);
    AtomicInteger interruptedCount = new AtomicInteger(0);
    CountDownLatch readyLatch = new CountDownLatch(5);

    var _ =
        strands.assertFails(
            InterruptedException.class,
            () -> {
              List<Strand<Void>> strandsList = new ArrayList<>();
              for (int i = 0; i < 5; i++) {
                strandsList.add(
                    async(
                        () -> {
                          startedCount.incrementAndGet();
                          readyLatch.countDown();
                          try {
                            Thread.sleep(60000);
                          } catch (InterruptedException e) {
                            interruptedCount.incrementAndGet();
                            throw e;
                          }
                          return null;
                        }));
              }
              readyLatch.await();
              Thread.currentThread().interrupt(); // Interrupt owner thread
              for (Strand<Void> strand : strandsList) {
                strand.await();
              }
              return null;
            });

    assertThat(startedCount.get()).isEqualTo(5);
    assertThat(interruptedCount.get()).isEqualTo(5);
  }

  @Test
  public void eventListenerThrows_scopeCloseCleansUpWithoutNPE() {
    EventListener throwingListener =
        new EventListener() {
          @Override
          public void scopeOpen() {
            throw new RuntimeException("Simulated scopeOpen failure");
          }

          @Override
          public void scopeClose(long durationNanos) {
            throw new RuntimeException("Simulated scopeClose failure");
          }

          @Override
          public void strandCreate() {}

          @Override
          public void strandReady(long delayNanos) {}

          @Override
          public void strandStart(long delayNanos) {}

          @Override
          public void strandComplete(long runtimeNanos, Strand.State state) {}
        };

    Environment customEnv =
        new Environment() {
          @Override
          public EventListener eventListener() {
            return throwingListener;
          }
        };

    StrandsRule customTester = StrandsRule.create(customEnv);

    // Verify task runs even if listener throws in scopeOpen/scopeClose without crashing with NPE
    int result = customTester.run(() -> async(() -> 42).await());
    assertThat(result).isEqualTo(42);
  }

  @Test
  public void frameworkThread_trackedInScope_interruptedOnScopeExit() throws Exception {
    CountDownLatch readyLatch = new CountDownLatch(1);
    CountDownLatch interruptLatch = new CountDownLatch(1);

    var _ =
        strands.run(
            () -> {
              Scope scope = Scope.current();
              scope.startFrameworkInternalThread(
                  () -> {
                    readyLatch.countDown();
                    try {
                      Thread.sleep(60000);
                    } catch (InterruptedException e) {
                      interruptLatch.countDown();
                    }
                  });
              readyLatch.await();
              return null;
            });

    interruptLatch.await();
    assertThat(interruptLatch.getCount()).isEqualTo(0);
  }

  @Test
  public void inScope_outsideScope_returnsFalse() {
    assertThat(Strands.inScope()).isFalse();
  }

  @Test
  public void inScope_insideScope_returnsTrue() {
    var _ =
        strands.run(
            () -> {
              assertThat(Strands.inScope()).isTrue();
              return null;
            });
  }
}
