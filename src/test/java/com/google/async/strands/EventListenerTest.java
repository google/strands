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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.async.strands.Strand.State;
import com.google.common.testing.TestLogHandler;
import com.google.common.util.concurrent.ListenableFuture;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class EventListenerTest {

  @Rule public final MockitoRule mocks = MockitoJUnit.rule();
  @Mock private EventListener mockListener;

  TestLogHandler logs;

  private Environment createTestEnvironment() {
    return new Environment() {

      @Override
      public EventListener eventListener() {
        return mockListener;
      }

      @Override
      public TimingMode timingMode() {
        return TimingMode.FULL;
      }
    };
  }

  @Before
  public void setUp() {
    logs = new TestLogHandler();
    Logger.getLogger(AsyncStrand.class.getName()).addHandler(logs);
    Logger.getLogger(Scope.class.getName()).addHandler(logs);
    logs.clear();
  }

  @After
  public void tearDown() {
    Logger.getLogger(AsyncStrand.class.getName()).removeHandler(logs);
    Logger.getLogger(Scope.class.getName()).removeHandler(logs);
  }

  @Test
  public void scopeEvents_success() throws Exception {
    ListenableFuture<Integer> result =
        Strands.concurrent(createTestEnvironment(), () -> scope(() -> 1));
    result.get();
    verify(mockListener, times(2)).scopeOpen();
    verify(mockListener, times(2)).scopeClose(anyLong());
    verify(mockListener, never()).strandCreate();
    verify(mockListener, never()).strandReady(anyLong());
    verify(mockListener, never()).strandStart(anyLong());
    verify(mockListener, never()).strandComplete(anyLong(), any(State.class));
    assertThat(logs.getStoredLogRecords()).isEmpty();
  }

  @Test
  public void strandEvents_success() throws Exception {
    ListenableFuture<Integer> result =
        Strands.concurrent(createTestEnvironment(), () -> async(() -> 1).await());
    result.get();
    verify(mockListener).scopeOpen();
    verify(mockListener).scopeClose(anyLong());
    verify(mockListener).strandCreate();
    verify(mockListener).strandReady(anyLong());
    verify(mockListener).strandStart(anyLong());
    verify(mockListener).strandComplete(anyLong(), eq(State.SUCCEEDED));
    assertThat(logs.getStoredLogRecords()).isEmpty();
  }

  @Test
  public void strandEvents_timingModeOff_receivesUntimedNanos() throws Exception {
    Environment env =
        new Environment() {
          @Override
          public EventListener eventListener() {
            return mockListener;
          }

          @Override
          public TimingMode timingMode() {
            return TimingMode.OFF;
          }
        };

    ListenableFuture<Integer> result = Strands.concurrent(env, () -> async(() -> 1).await());
    result.get();
    verify(mockListener).scopeOpen();
    verify(mockListener).scopeClose(eq(EventListener.UNTIMED_NANOS));
    verify(mockListener).strandCreate();
    verify(mockListener).strandReady(eq(EventListener.UNTIMED_NANOS));
    verify(mockListener).strandStart(eq(EventListener.UNTIMED_NANOS));
    verify(mockListener).strandComplete(eq(EventListener.UNTIMED_NANOS), eq(State.SUCCEEDED));
    assertThat(logs.getStoredLogRecords()).isEmpty();
  }

  @Test
  public void strandEvents_failure() throws Exception {
    ListenableFuture<Integer> result =
        Strands.concurrent(
            createTestEnvironment(),
            () -> {
              Strand<Integer> s1 =
                  async(
                      () -> {
                        if (true) {
                          throw new RuntimeException("failed");
                        }
                        return 1;
                      });
              return s1.await();
            });
    ExecutionException e = assertThrows(ExecutionException.class, () -> result.get());
    assertThat(e).hasCauseThat().isInstanceOf(RuntimeException.class);
    verify(mockListener).scopeOpen();
    verify(mockListener).scopeClose(anyLong());
    verify(mockListener).strandCreate();
    verify(mockListener).strandReady(anyLong());
    verify(mockListener).strandStart(anyLong());
    verify(mockListener).strandComplete(anyLong(), eq(State.FAILED));
    assertThat(logs.getStoredLogRecords()).isEmpty();
  }

  @Test
  public void strandEvents_cancelled() throws Exception {
    CountDownLatch running = new CountDownLatch(1);
    CountDownLatch cancelled = new CountDownLatch(1);
    ListenableFuture<Integer> result =
        Strands.concurrent(
            createTestEnvironment(),
            () -> {
              Strand<Integer> s1 =
                  async(
                      () -> {
                        running.countDown();
                        cancelled.await();
                        return 1;
                      });
              var _ =
                  async(
                      () -> {
                        running.await();
                        s1.cancel();
                        return 1;
                      });
              return s1.await();
            });
    ExecutionException e = assertThrows(ExecutionException.class, () -> result.get());
    assertThat(e).hasCauseThat().isInstanceOf(CancellationException.class);
    verify(mockListener).scopeOpen();
    verify(mockListener).scopeClose(anyLong());
    verify(mockListener, times(2)).strandCreate();
    verify(mockListener, times(2)).strandReady(anyLong());
    verify(mockListener, times(2)).strandStart(anyLong());
    verify(mockListener).strandComplete(anyLong(), eq(State.CANCELLED));
    verify(mockListener).strandComplete(anyLong(), eq(State.SUCCEEDED));
    assertThat(logs.getStoredLogRecords()).isEmpty();
  }

  @Test
  public void strandEvents_timeout() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    ListenableFuture<Integer> result =
        Strands.concurrent(
            createTestEnvironment(),
            () -> {
              Strand<Integer> s1 =
                  async(
                      () -> {
                        latch.await();
                        return 1;
                      });
              return s1.await(Duration.ofMillis(100));
            });
    ExecutionException e = assertThrows(ExecutionException.class, () -> result.get());
    assertThat(e).hasCauseThat().isInstanceOf(TimeoutException.class);
    verify(mockListener).scopeOpen();
    verify(mockListener).scopeClose(anyLong());
    verify(mockListener).strandCreate();
    verify(mockListener).strandReady(anyLong());
    verify(mockListener).strandStart(anyLong());
    verify(mockListener).strandComplete(anyLong(), eq(State.TIMEOUT));
    assertThat(logs.getStoredLogRecords()).isEmpty();
  }

  @Test
  public void strandEvents_exceptionInListener_doesNotAffectExecution() throws Exception {
    doThrow(new RuntimeException("failed"))
        .when(mockListener)
        .strandComplete(anyLong(), any(State.class));
    ListenableFuture<Integer> result =
        Strands.concurrent(createTestEnvironment(), () -> async(() -> 1).await());
    result.get();
    verify(mockListener).scopeOpen();
    verify(mockListener).scopeClose(anyLong());
    verify(mockListener).strandCreate();
    verify(mockListener).strandReady(anyLong());
    verify(mockListener).strandStart(anyLong());
    verify(mockListener).strandComplete(anyLong(), eq(State.SUCCEEDED));
    assertThat(logs.getStoredLogRecords()).hasSize(1);
    assertThat(logs.getStoredLogRecords().get(0).getMessage()).contains("strandComplete");
  }

  @Test
  public void strandEvents_race_success() throws Exception {
    CountDownLatch wait = new CountDownLatch(1);
    CountDownLatch start = new CountDownLatch(1);
    ListenableFuture<Integer> result =
        Strands.concurrent(
            createTestEnvironment(),
            () -> {
              Strand<Integer> s1 =
                  async(
                      () -> {
                        wait.await();
                        return 1;
                      });
              Strand<Integer> s2 =
                  async(
                      () -> {
                        wait.await();
                        return 2;
                      });
              Strand<Integer> s3 =
                  async(
                      () -> {
                        start.await();
                        return 3;
                      });
              return compose(s1, s2, s3).firstSuccessful().await();
            });
    start.countDown();
    assertThat(result.get()).isEqualTo(3);
    verify(mockListener).scopeOpen();
    verify(mockListener).scopeClose(anyLong());
    verify(mockListener, times(4)).strandCreate();
    verify(mockListener, times(4)).strandReady(anyLong());
    verify(mockListener, times(4)).strandStart(anyLong());
    verify(mockListener, times(2)).strandComplete(anyLong(), eq(State.SUCCEEDED));
    verify(mockListener, times(2)).strandComplete(anyLong(), eq(State.CANCELLED));
    assertThat(logs.getStoredLogRecords()).isEmpty();
  }
}
