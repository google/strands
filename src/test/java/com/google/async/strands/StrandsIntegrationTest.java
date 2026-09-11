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
import static com.google.async.strands.Strands.compose;
import static com.google.async.strands.Strands.scope;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.async.strands.testing.StrandsRule;
import com.google.async.strands.testing.StrandsRule.InStrand;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@InStrand
@RunWith(JUnit4.class)
public class StrandsIntegrationTest {

  @Rule public final StrandsRule strands = StrandsRule.create();

  @Test
  public void async_singleStrand_succeeds() throws Exception {
    int result = async(() -> 1).await();

    assertThat(result).isEqualTo(1);
  }

  @Test
  public void async_multipleStrands_succeeds() throws Exception {
    var s1 = async(() -> 1);
    var s2 = async(() -> 3);

    int result = s1.await() + s2.await();

    assertThat(result).isEqualTo(4);
  }

  @Test
  public void await_withTimeout_throwsTimeoutException() {
    AtomicReference<Strand<Integer>> s1Ref = new AtomicReference<>();

    strands.assertFails(
        TimeoutException.class,
        () -> {
          var s1 =
              async(
                  () -> {
                    Thread.sleep(20000);
                    return 1;
                  });
          s1Ref.set(s1);
          return s1.await(Duration.ofMillis(100));
        });

    assertThat(s1Ref.get().state()).isEqualTo(Strand.State.TIMEOUT);
  }

  @Test
  public void firstSuccessful_withMultipleCandidates_returnsFastest() throws Exception {
    var a =
        async(
            () -> {
              Thread.sleep(20000);
              return 1;
            });
    var b =
        async(
            () -> {
              Thread.sleep(15000);
              return 2;
            });
    var c = async(() -> 3);

    int result = compose(a, b, c).firstSuccessful().await();

    assertThat(result).isEqualTo(3);
  }

  @Test
  public void async_withFuture_succeeds() throws Exception {
    int result = async(Executors.newSingleThreadExecutor().submit(() -> 1)).await();

    assertThat(result).isEqualTo(1);
  }

  @Test
  public void scope_singleScope_succeeds() throws Exception {
    int result = scope(() -> 1);

    assertThat(result).isEqualTo(1);
  }

  @Test
  public void scope_withAsync_succeeds() throws Exception {
    int result =
        scope(
            () -> {
              var s1 = async(() -> 1);
              var s2 = async(() -> 3);
              return s1.await() + s2.await();
            });

    assertThat(result).isEqualTo(4);
  }

  @Test
  public void scope_nested_succeeds() throws Exception {
    int result =
        scope(
            () -> {
              return 1 + scope(() -> 2);
            });

    assertThat(result).isEqualTo(3);
  }

  @Test
  public void async_taskThrowsException_strandFails() {
    AtomicReference<Strand<Integer>> s1Ref = new AtomicReference<>();

    RuntimeException e =
        strands.assertFails(
            RuntimeException.class,
            () -> {
              Strand<Integer> s1 =
                  async(
                      () -> {
                        throw new RuntimeException("failed");
                      });
              s1Ref.set(s1);
              return s1.await();
            });

    assertThat(e).hasMessageThat().isEqualTo("failed");
    assertThat(s1Ref.get().state()).isEqualTo(Strand.State.FAILED);
  }

  @Test
  public void async_futureThrowsException_strandFails() {
    AtomicReference<Strand<Integer>> s1Ref = new AtomicReference<>();

    RuntimeException e =
        strands.assertFails(
            RuntimeException.class,
            () -> {
              Callable<Integer> task =
                  () -> {
                    throw new RuntimeException("failed");
                  };
              var s1 = async(Executors.newSingleThreadExecutor().submit(task));
              s1Ref.set(s1);
              return s1.await();
            });

    assertThat(e).hasMessageThat().isEqualTo("failed");
    assertThat(s1Ref.get().state()).isEqualTo(Strand.State.FAILED);
  }

  @Test
  public void scope_taskThrowsException_throwsExceptionDirectly() {
    strands.assertFails(
        IllegalArgumentException.class,
        () ->
            scope(
                () -> {
                  throw new IllegalArgumentException("failed");
                }));
  }

  @Test
  public void async_cancel_strandIsCancelled() {
    AtomicReference<Strand<Integer>> s1Ref = new AtomicReference<>();

    strands.assertFails(
        CancellationException.class,
        () -> {
          var s1 =
              async(
                  () -> {
                    Thread.sleep(20000);
                    return 1;
                  });
          s1Ref.set(s1);
          s1.cancel();
          return s1.await();
        });

    assertThat(s1Ref.get().state()).isEqualTo(Strand.State.CANCELLED);
  }

  @Test
  public void firstSuccessful_allCandidatesFail_failsWithFirstFailure() {
    AtomicReference<Strand<Integer>> aRef = new AtomicReference<>();
    AtomicReference<Strand<Integer>> bRef = new AtomicReference<>();
    RuntimeException aException = new RuntimeException("a failed");
    RuntimeException bException = new RuntimeException("b failed");

    RuntimeException e =
        strands.assertFails(
            RuntimeException.class,
            () -> {
              Strand<Integer> a =
                  async(
                      () -> {
                        Thread.sleep(100);
                        throw aException;
                      });
              aRef.set(a);
              Strand<Integer> b =
                  async(
                      () -> {
                        Thread.sleep(200);
                        throw bException;
                      });
              bRef.set(b);
              return compose(a, b).firstSuccessful().await();
            });

    assertThat(e).isSameInstanceAs(aException);
    assertThat(e.getSuppressed()).asList().containsExactly(bException);
    assertThat(aRef.get().state()).isEqualTo(Strand.State.FAILED);
    assertThat(bRef.get().state()).isEqualTo(Strand.State.FAILED);
  }

  @Test
  public void firstSuccessful_withOneCandidate_succeeds() throws Exception {
    int result =
        scope(
            () -> {
              var s1 = async(() -> 1);
              return compose(s1).firstSuccessful().await();
            });

    assertThat(result).isEqualTo(1);
  }

  @Test
  public void firstSuccessful_withNoCandidates_throwsIllegalArgumentException() {
    strands.assertFails(IllegalArgumentException.class, () -> compose().firstSuccessful().await());
  }

  @Test
  public void firstSuccessful_oneFailsBeforeSuccess_returnsSuccess() throws Exception {
    AtomicReference<Strand<Integer>> aRef = new AtomicReference<>();
    AtomicReference<Strand<Integer>> bRef = new AtomicReference<>();

    int result =
        scope(
            () -> {
              var a =
                  async(
                      () -> {
                        Thread.sleep(100);
                        if (true) {
                          throw new RuntimeException("a failed");
                        }
                        return 1;
                      });
              aRef.set(a);
              var b =
                  async(
                      () -> {
                        Thread.sleep(200);
                        return 1;
                      });
              bRef.set(b);
              return compose(a, b).firstSuccessful().await();
            });

    assertThat(result).isEqualTo(1);
    assertThat(aRef.get().state()).isEqualTo(Strand.State.FAILED);
    assertThat(bRef.get().state()).isEqualTo(Strand.State.SUCCEEDED);
  }

  @Test
  public void firstSuccessful_cancel_strandIsCancelled() {
    AtomicReference<Strand<Integer>> firstRef = new AtomicReference<>();

    strands.assertFails(
        CancellationException.class,
        () -> {
          var s1 =
              async(
                  () -> {
                    Thread.sleep(20000);
                    return 1;
                  });
          var firstStrand = compose(s1).firstSuccessful();
          firstRef.set(firstStrand);
          firstStrand.cancel();
          return firstStrand.await();
        });

    assertThat(firstRef.get().state()).isEqualTo(Strand.State.CANCELLED);
  }

  @Test
  public void scope_asyncTimesOut_propagatesTimeoutException() {
    AtomicReference<Strand<Integer>> s1Ref = new AtomicReference<>();

    strands.assertFails(
        TimeoutException.class,
        () ->
            scope(
                () -> {
                  var s1 =
                      async(
                          () -> {
                            Thread.sleep(20000);
                            return 1;
                          });
                  s1Ref.set(s1);
                  return s1.await(Duration.ofMillis(100));
                }));

    assertThat(s1Ref.get().state()).isEqualTo(Strand.State.TIMEOUT);
  }

  @Test
  public void async_withScope_succeeds() throws Exception {
    int result =
        async(
                () -> {
                  return scope(() -> 1);
                })
            .await();

    assertThat(result).isEqualTo(1);
  }

  @Test
  public void async_nested_succeeds() throws Exception {
    var s1 = async(() -> 1);
    var s2 = async(() -> 3 + s1.await());

    int result = s2.await();

    assertThat(result).isEqualTo(4);
  }

  @Test
  public void await_withTimeout_succeedsBeforeTimeout() throws Exception {
    var s1 = async(() -> 1);

    int result = s1.await(Duration.ofSeconds(1));

    assertThat(result).isEqualTo(1);
  }

  @Test
  public void scope_taskThrowsInterruptedException_propagatesException() {
    strands.assertFails(
        InterruptedException.class,
        () ->
            scope(
                () -> {
                  throw new InterruptedException();
                }));
  }

  @Test
  public void async_taskThrowsInterruptedException_strandFails() {
    AtomicReference<Strand<Integer>> s1Ref = new AtomicReference<>();

    strands.assertFails(
        InterruptedException.class,
        () -> {
          Strand<Integer> s1 =
              async(
                  () -> {
                    throw new InterruptedException();
                  });
          s1Ref.set(s1);
          return s1.await();
        });

    assertThat(s1Ref.get().state()).isEqualTo(Strand.State.INTERRUPTED);
  }

  @Test
  public void await_threadInterrupted_throwsInterruptedException() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    ListenableFuture<Integer> result =
        Strands.concurrent(
            () -> {
              threadRef.set(Thread.currentThread());
              return async(
                      () -> {
                        latch.await();
                        return 1;
                      })
                  .await();
            });

    while (threadRef.get() == null) {
      Thread.sleep(10);
    }
    Thread strandThread = threadRef.get();
    strandThread.interrupt();

    var e = assertThrows(ExecutionException.class, result::get);
    assertThat(e).hasCauseThat().isInstanceOf(InterruptedException.class);
    // InterruptedException should be thrown by await(), and Strand thread should have interrupted
    // status set.
    assertThat(strandThread.isInterrupted()).isTrue();

    latch.countDown(); // release async strand
  }

  @Test
  public void firstSuccessful_candidateThrowsInterruptedException_strandFails() {
    AtomicReference<Strand<Integer>> s1Ref = new AtomicReference<>();

    strands.assertFails(
        InterruptedException.class,
        () -> {
          Strand<Integer> s1 =
              async(
                  () -> {
                    throw new InterruptedException();
                  });
          s1Ref.set(s1);
          return compose(s1).firstSuccessful().await();
        });

    assertThat(s1Ref.get().state()).isEqualTo(Strand.State.INTERRUPTED);
  }

  @Test
  public void scope_exitWithActiveChildStrands_interruptsChildren() throws Exception {
    AtomicReference<Strand<Void>> s1Ref = new AtomicReference<>();

    var _ =
        scope(
            () -> {
              s1Ref.set(
                  async(
                      () -> {
                        Thread.sleep(30000);
                        return null;
                      }));
              return 1; // scope returns immediately
            });
    Thread.sleep(100);

    assertThat(s1Ref.get().state()).isEqualTo(Strand.State.INTERRUPTED);
  }

  @Test
  public void async_cancelFuture_cancelsFuture() {
    SettableFuture<Integer> future = SettableFuture.create();
    AtomicReference<Strand<Integer>> s1Ref = new AtomicReference<>();

    strands.assertFails(
        CancellationException.class,
        () -> {
          Strand<Integer> s1 = async(future);
          s1Ref.set(s1);
          s1.cancel();
          return s1.await();
        });

    assertThat(s1Ref.get().state()).isEqualTo(Strand.State.CANCELLED);
    assertThat(future.isCancelled()).isTrue();
  }

  @Test
  public void async_futureThrowsInterruptedException_strandFails() {
    AtomicReference<Strand<Integer>> s1Ref = new AtomicReference<>();

    strands.assertFails(
        InterruptedException.class,
        () -> {
          var s1 =
              async(
                  Executors.newSingleThreadExecutor()
                      .submit(
                          (Callable<Integer>)
                              () -> {
                                throw new InterruptedException();
                              }));
          s1Ref.set(s1);
          return s1.await();
        });

    assertThat(s1Ref.get().state()).isEqualTo(Strand.State.INTERRUPTED);
  }

  @Test
  public void async_mutableStateSharedAcrossStrands_isSerialized() throws Exception {
    // Using a plain int in an array to represent non-thread-safe mutable state.
    // If Strands did not provide a serialization guarantee, concurrent modification
    // of counter[0] would lead to lost updates from first conditions.
    final int[] counter = new int[] {0};
    int incrementsPerStrand = 10000;
    int strandCount = 10;

    List<Strand<Void>> strandsList = new ArrayList<>();
    for (int i = 0; i < strandCount; i++) {
      strandsList.add(
          async(
              () -> {
                for (int j = 0; j < incrementsPerStrand; j++) {
                  counter[0]++;
                }
                return null;
              }));
    }
    for (Strand<Void> strand : strandsList) {
      strand.await();
    }

    assertThat(counter[0]).isEqualTo(strandCount * incrementsPerStrand);
  }

  @Test
  public void allSuccessful_withMultipleCandidates_succeeds() throws Exception {
    var a = async(() -> 1);
    var b = async(() -> 2);
    var c = async(() -> 3);

    Stream<Integer> result = compose(a, b, c).allSuccessful().await();

    assertThat(result).containsExactly(1, 2, 3).inOrder();
  }

  @Test
  public void allSuccessful_withMultipleCandidatesAsIterable_succeeds() throws Exception {
    var a = async(() -> 1);
    var b = async(() -> 2);
    var c = async(() -> 3);

    Stream<Integer> result = compose(a, b, c).allSuccessful().await();

    assertThat(result).containsExactly(1, 2, 3).inOrder();
  }

  @Test
  public void allSuccessful_withOneCandidate_succeeds() throws Exception {
    var s1 = async(() -> 1);

    Stream<Integer> result = compose(s1).allSuccessful().await();

    assertThat(result).containsExactly(1);
  }

  @Test
  public void allSuccessful_withNoCandidates_throwsIllegalArgumentException() {
    strands.assertFails(
        IllegalArgumentException.class,
        () -> {
          return compose().allSuccessful().await();
        });
  }

  @Test
  public void allSuccessful_oneFails_failsWithFirstFailure() {
    AtomicReference<Strand<Integer>> aRef = new AtomicReference<>();
    AtomicReference<Strand<Integer>> bRef = new AtomicReference<>();
    RuntimeException e =
        strands.assertFails(
            RuntimeException.class,
            () -> {
              Strand<Integer> a =
                  async(
                      () -> {
                        Thread.sleep(100);
                        throw new RuntimeException("a failed");
                      });
              aRef.set(a);
              Strand<Integer> b =
                  async(
                      () -> {
                        Thread.sleep(20000);
                        return 2;
                      });
              bRef.set(b);
              return compose(a, b).allSuccessful().await();
            });

    assertThat(e).hasMessageThat().isEqualTo("a failed");
    assertThat(aRef.get().state()).isEqualTo(Strand.State.FAILED);
  }

  @Test
  public void allSuccessful_cancel_strandIsCancelled() {
    AtomicReference<Strand<Stream<Integer>>> streamRef = new AtomicReference<>();

    strands.assertFails(
        CancellationException.class,
        () -> {
          var s1 =
              async(
                  () -> {
                    Thread.sleep(20000);
                    return 1;
                  });
          var s2 =
              async(
                  () -> {
                    Thread.sleep(20000);
                    return 1;
                  });
          var strand = compose(s1, s2).allSuccessful();
          streamRef.set(strand);
          strand.cancel();
          return strand.await();
        });

    assertThat(streamRef.get().state()).isEqualTo(Strand.State.CANCELLED);
  }

  @Test
  public void await_outOfScopeParent_throwsIllegalStateException() {
    strands.assertFails(
        IllegalStateException.class,
        () -> {
          var strand = new AtomicReference<Strand<Integer>>(null);
          var unusedVal =
              scope(
                  () -> {
                    strand.set(async(() -> 1));
                    return 1;
                  });
          return strand.get().await();
        });
  }

  @Test
  public void await_outOfScopeChild_throwsIllegalStateException() {
    strands.assertFails(
        IllegalStateException.class,
        () -> {
          var strand = async(() -> 1);
          return scope(() -> strand.await());
        });
  }

  @Test
  public void compose_outOfScopeCandidate_throwsIllegalArgumentException() {
    strands.assertFails(
        IllegalArgumentException.class,
        () -> {
          var strand = new AtomicReference<Strand<Integer>>(null);
          var unusedVal =
              scope(
                  () -> {
                    strand.set(async(() -> 1));
                    return 1;
                  });
          return compose(strand.get()).firstSuccessful().await();
        });
  }
}
