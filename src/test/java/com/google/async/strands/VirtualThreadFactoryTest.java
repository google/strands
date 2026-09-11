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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class VirtualThreadFactoryTest {

  @Test
  public void task_createsVirtualThreadsWithIncrementingTaskNames() {
    VirtualThreadFactory factory = VirtualThreadFactory.task();

    Thread t1 = factory.newVirtualThread(() -> {});
    Thread t2 = factory.newVirtualThread(() -> {});

    assertThat(t1.isVirtual()).isTrue();
    assertThat(t1.isAlive()).isFalse();
    assertThat(t1.getName()).isEqualTo("Strands-Task-Virtual-1");
    assertThat(t2.getName()).isEqualTo("Strands-Task-Virtual-2");
  }

  @Test
  public void framework_createsVirtualThreadsWithFrameworkNames() {
    VirtualThreadFactory factory = VirtualThreadFactory.framework();

    Thread t1 = factory.newVirtualThread(() -> {});

    assertThat(t1.isVirtual()).isTrue();
    assertThat(t1.isAlive()).isFalse();
    assertThat(t1.getName()).startsWith("Strands-Framework-Virtual-");
  }

  @Test
  public void framework_returnsSingletonInstance() {
    assertThat(VirtualThreadFactory.framework()).isSameInstanceAs(VirtualThreadFactory.framework());
  }

  @Test
  public void task_returnsDistinctInstances() {
    assertThat(VirtualThreadFactory.task()).isNotSameInstanceAs(VirtualThreadFactory.task());
  }

  @Test
  public void newVirtualThread_withNullRunnable_throwsNullPointerException() {
    VirtualThreadFactory factory = VirtualThreadFactory.task();
    assertThrows(NullPointerException.class, () -> factory.newVirtualThread(null));
  }

  @Test
  public void newVirtualThread_providesSingleExecutionGuaranteeAcrossVirtualThreads()
      throws Exception {
    VirtualThreadFactory factory = VirtualThreadFactory.task();
    int numTasks = 1000;
    CountDownLatch latch = new CountDownLatch(numTasks);
    int[] counter = new int[1];
    AtomicBoolean overlapDetected = new AtomicBoolean(false);
    AtomicBoolean isExecuting = new AtomicBoolean(false);

    for (int i = 0; i < numTasks; i++) {
      Thread thread =
          factory.newVirtualThread(
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
      thread.start();
    }

    latch.await();
    assertThat(overlapDetected.get()).isFalse();
    assertThat(counter[0]).isEqualTo(numTasks * 1000);
  }
}
