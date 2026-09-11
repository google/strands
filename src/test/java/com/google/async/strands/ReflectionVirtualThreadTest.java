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
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ReflectionVirtualThreadTest {

  private VirtualThreadSupport.Factory factory;

  @Before
  public void setUp() {
    assertThat(VirtualThreadSupport.isJavaLangOpen()).isTrue();
    factory = VirtualThreadSupport.ReflectionFactory.create();
    assertThat(factory).isInstanceOf(VirtualThreadSupport.ReflectionFactory.class);
  }

  @Test
  public void reflectionFactory_createsVirtualThreadOnCustomCarrier() throws Exception {
    AtomicBoolean carrierUsed = new AtomicBoolean(false);
    Executor customCarrier =
        command -> {
          carrierUsed.set(true);
          ForkJoinPool.commonPool().execute(command);
        };

    CountDownLatch done = new CountDownLatch(1);
    Thread thread = factory.create(customCarrier).start(done::countDown);

    assertThat(thread.isVirtual()).isTrue();
    assertThat(done.await(5, SECONDS)).isTrue();
    thread.join();
    assertThat(carrierUsed.get()).isTrue();
  }

  @Test
  public void reflectionFactory_withSequentialExecutor_enforcesMutualExclusion() throws Exception {
    Thread.Builder.OfVirtual builder =
        factory.create(new SequentialExecutor(ForkJoinPool.commonPool()));

    int taskCount = 500;
    CountDownLatch allDone = new CountDownLatch(taskCount);
    AtomicBoolean overlapDetected = new AtomicBoolean(false);
    AtomicBoolean isExecuting = new AtomicBoolean(false);
    int[] counter = new int[1];

    for (int i = 0; i < taskCount; i++) {
      builder.start(
          () -> {
            if (isExecuting.getAndSet(true)) {
              overlapDetected.set(true);
            }
            for (int j = 0; j < 100; j++) {
              counter[0]++;
            }
            isExecuting.set(false);
            allDone.countDown();
          });
    }

    assertThat(allDone.await(5, SECONDS)).isTrue();
    assertThat(overlapDetected.get()).isFalse();
    assertThat(counter[0]).isEqualTo(taskCount * 100);
  }
}
