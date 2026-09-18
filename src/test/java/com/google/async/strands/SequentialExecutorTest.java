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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SequentialExecutorTest {

  @Test
  public void execute_runsTasksInFifoOrder() throws Exception {
    SequentialExecutor executor = new SequentialExecutor(ForkJoinPool.commonPool());
    List<Integer> executed = new CopyOnWriteArrayList<>();
    int numTasks = 100;
    CountDownLatch latch = new CountDownLatch(numTasks);

    for (int i = 0; i < numTasks; i++) {
      int val = i;
      executor.execute(
          () -> {
            executed.add(val);
            latch.countDown();
          });
    }

    latch.await();
    for (int i = 0; i < numTasks; i++) {
      assertThat(executed.get(i)).isEqualTo(i);
    }
  }

  @Test
  public void execute_enforcesMutualExclusionUnderHighConcurrency() throws Exception {
    SequentialExecutor executor = new SequentialExecutor(Executors.newFixedThreadPool(8));
    int numTasks = 1000;
    CountDownLatch latch = new CountDownLatch(numTasks);
    int[] counter = new int[1];
    AtomicBoolean overlapDetected = new AtomicBoolean(false);
    AtomicBoolean isExecuting = new AtomicBoolean(false);

    for (int i = 0; i < numTasks; i++) {
      executor.execute(
          () -> {
            if (isExecuting.getAndSet(true)) {
              overlapDetected.set(true);
            }
            for (int j = 0; j < 1000; j++) {
              counter[0]++;
            }
            isExecuting.set(false);
            latch.countDown();
          });
    }

    latch.await();
    assertThat(overlapDetected.get()).isFalse();
    assertThat(counter[0]).isEqualTo(numTasks * 1000);
  }

  @Test
  public void execute_whenTaskThrowsRuntimeException_subsequentTasksContinue() throws Exception {
    SequentialExecutor executor = new SequentialExecutor(ForkJoinPool.commonPool());
    CountDownLatch latch = new CountDownLatch(2);
    AtomicBoolean secondRan = new AtomicBoolean(false);

    executor.execute(
        () -> {
          latch.countDown();
          throw new RuntimeException("Simulated task failure");
        });
    executor.execute(
        () -> {
          secondRan.set(true);
          latch.countDown();
        });

    latch.await();
    assertThat(secondRan.get()).isTrue();
  }

  @Test
  public void execute_nullTask_throwsNullPointerException() {
    SequentialExecutor executor = new SequentialExecutor(ForkJoinPool.commonPool());
    assertThrows(NullPointerException.class, () -> executor.execute(null));
  }

  @Test
  public void toString_containsDelegate() {
    ForkJoinPool pool = ForkJoinPool.commonPool();
    SequentialExecutor executor = new SequentialExecutor(pool);
    assertThat(executor.toString()).contains(pool.toString());
  }
}
