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

import com.google.async.strands.testing.StrandsRule;
import com.google.async.strands.testing.StrandsRule.InStrand;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AsyncStrandTest {

  @Rule public final StrandsRule strands = StrandsRule.create();

  @Test
  public void await_fromOutsideScope_throwsIllegalStateException() {
    AtomicReference<Strand<Integer>> strandRef = new AtomicReference<>();
    var _ =
        strands.run(
            () -> {
              Strand<Integer> strand = Strands.async(() -> 42);
              strandRef.set(strand);
              return null;
            });

    // Attempting to await the strand outside of the scope that created it
    Strand<Integer> strand = strandRef.get();
    assertThrows(IllegalStateException.class, strand::await);
  }

  @Test
  @InStrand
  public void await_callerThreadInterrupted_cancelsTaskAndRestoresInterrupt() throws Exception {
    CountDownLatch taskStarted = new CountDownLatch(1);
    CountDownLatch taskInterruptedLatch = new CountDownLatch(1);

    Strand<Void> strand =
        Strands.async(
            () -> {
              taskStarted.countDown();
              try {
                Thread.sleep(60000);
              } catch (InterruptedException e) {
                taskInterruptedLatch.countDown();
                throw e;
              }
              return null;
            });

    taskStarted.await();
    // Interrupt the current thread while awaiting
    Thread.currentThread().interrupt();

    assertThrows(InterruptedException.class, () -> strand.await(Duration.ofMinutes(1)));
    assertThat(Thread.interrupted()).isTrue(); // Restored interrupted status
    taskInterruptedLatch.await();
    assertThat(taskInterruptedLatch.getCount()).isEqualTo(0);
    assertThat(strand.state()).isEqualTo(Strand.State.INTERRUPTED);
  }

  @Test
  @InStrand
  public void pushCallback_afterStrandCompletion_returnsFalse() throws Exception {
    AsyncStrand<Integer> strand = (AsyncStrand<Integer>) Strands.async(() -> 100);
    var _ = strand.await(); // Wait for strand to complete and set TOMBSTONE

    AtomicBoolean callbackRan = new AtomicBoolean(false);
    boolean pushed = strand.pushCallback(() -> callbackRan.set(true));

    assertThat(pushed).isFalse();
    assertThat(callbackRan.get()).isFalse();
  }

  @Test
  public void automaton_concurrentCancelAndSucceed_atomicallyEnforcesSingleWinner()
      throws Exception {
    for (int iter = 0; iter < 50; iter++) {
      CountDownLatch readyLatch = new CountDownLatch(1);
      CountDownLatch startLatch = new CountDownLatch(1);
      AtomicReference<Object> resultRef = new AtomicReference<>();

      var _ =
          strands.run(
              () -> {
                AsyncStrand<Integer> strand =
                    new AsyncStrand<>(
                        Scope.current(),
                        () -> {
                          readyLatch.countDown();
                          startLatch.await();
                          return 777;
                        });
                strand.start();

                readyLatch.await();

                // Race cancel() against task execution finish
                Thread cancelThread = Thread.ofVirtual().start(strand::cancel);
                startLatch.countDown();
                cancelThread.join();

                try {
                  resultRef.set(strand.await());
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  resultRef.set(e);
                } catch (Throwable t) {
                  resultRef.set(t);
                }
                return null;
              });

      Object res = resultRef.get();
      // Result must be EITHER 777 (Succeeded) OR FailedTaskException wrapping
      // CancellationException.
      if (res instanceof FailedTaskException fte) {
        assertThat(fte).hasCauseThat().isInstanceOf(CancellationException.class);
      } else {
        assertThat(res).isEqualTo(777);
      }
    }
  }

  @Test
  @InStrand
  public void state_transitions_fromCreatedToSucceeded() throws Exception {
    AsyncStrand<String> strand = new AsyncStrand<>(Scope.current(), () -> "successResult");
    assertThat(strand.state()).isEqualTo(Strand.State.CREATED);

    strand.start();
    String val = strand.await();

    assertThat(val).isEqualTo("successResult");
    assertThat(strand.state()).isEqualTo(Strand.State.SUCCEEDED);
  }

  @Test
  @InStrand
  public void state_transitions_taskThrowsException_transitionsToFailed() {
    AsyncStrand<String> strand =
        new AsyncStrand<>(
            Scope.current(),
            () -> {
              throw new IllegalArgumentException("Invalid argument test");
            });
    strand.start();

    FailedTaskException ex = assertThrows(FailedTaskException.class, strand::await);
    assertThat(ex).hasCauseThat().isInstanceOf(IllegalArgumentException.class);
    assertThat(ex).hasCauseThat().hasMessageThat().contains("Invalid argument test");
    assertThat(strand.state()).isEqualTo(Strand.State.FAILED);
  }

  @Test
  @InStrand
  public void cancel_runningTask_transitionsToCancelled() throws Exception {
    CountDownLatch startedLatch = new CountDownLatch(1);
    Strand<Integer> strand =
        Strands.async(
            () -> {
              startedLatch.countDown();
              Thread.sleep(60000);
              return 42;
            });

    startedLatch.await();
    strand.cancel();

    FailedTaskException ex = assertThrows(FailedTaskException.class, strand::await);
    assertThat(ex).hasCauseThat().isInstanceOf(CancellationException.class);
    assertThat(strand.state()).isEqualTo(Strand.State.CANCELLED);
  }

  @Test
  @InStrand
  public void awaitWithTimeout_timedOut_transitionsToTimeoutAndThrowsFailedTaskException()
      throws Exception {
    AsyncStrand<Void> strand =
        new AsyncStrand<>(
            Scope.current(),
            () -> {
              Thread.sleep(60000);
              return null;
            });
    strand.start();

    FailedTaskException ex =
        assertThrows(FailedTaskException.class, () -> strand.await(Duration.ofMillis(50)));
    assertThat(ex).hasCauseThat().isInstanceOf(TimeoutException.class);
    assertThat(strand.state()).isEqualTo(Strand.State.TIMEOUT);
  }

  @Test
  @InStrand
  public void pushCallback_multipleCallbacks_executedInLifoOrder() throws Exception {
    AsyncStrand<Integer> strand = new AsyncStrand<>(Scope.current(), () -> 100);
    List<Integer> executionOrder = new ArrayList<>();

    assertThat(strand.pushCallback(() -> executionOrder.add(1))).isTrue();
    assertThat(strand.pushCallback(() -> executionOrder.add(2))).isTrue();
    assertThat(strand.pushCallback(() -> executionOrder.add(3))).isTrue();

    strand.start();
    var _ = strand.await();

    assertThat(executionOrder).containsExactly(3, 2, 1).inOrder();
  }

  @Test
  @InStrand
  public void taskReturningNull_supportedSuccessfully() throws Exception {
    AsyncStrand<String> strand = new AsyncStrand<>(Scope.current(), () -> null);
    strand.start();

    String val = strand.await();
    assertThat(val).isNull();
    assertThat(strand.state()).isEqualTo(Strand.State.SUCCEEDED);
  }

  @Test
  @InStrand
  public void cancel_beforeExecute_transitionsToCancelledAndSkipsTaskExecution() {
    CountDownLatch startLatch = new CountDownLatch(1);
    AtomicBoolean taskExecuted = new AtomicBoolean(false);
    AsyncStrand<Void> strand =
        new AsyncStrand<>(
            Scope.current(),
            () -> {
              startLatch.await();
              taskExecuted.set(true);
              return null;
            });
    strand.start();
    strand.cancel();
    startLatch.countDown();

    FailedTaskException ex = assertThrows(FailedTaskException.class, strand::await);
    assertThat(ex).hasCauseThat().isInstanceOf(CancellationException.class);
    assertThat(strand.state()).isEqualTo(Strand.State.CANCELLED);
    assertThat(taskExecuted.get()).isFalse();
  }
}
