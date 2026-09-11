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
import static org.junit.Assert.assertThrows;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class VirtualThreadSupportTest {

  @Test
  public void taskThreadFactory_returnsWorkingFactory() throws Exception {
    ThreadFactory factory = VirtualThreadSupport.taskThreadFactory();
    CountDownLatch latch = new CountDownLatch(1);
    Thread thread = factory.newThread(latch::countDown);

    assertThat(thread.isVirtual()).isTrue();
    assertThat(thread.getName()).startsWith("Strands-Task-Virtual-");
    thread.start();
    latch.await();
  }

  @Test
  public void frameworkThreadFactory_returnsWorkingFactory() throws Exception {
    ThreadFactory factory = VirtualThreadSupport.frameworkThreadFactory();
    CountDownLatch latch = new CountDownLatch(1);
    Thread thread = factory.newThread(latch::countDown);

    assertThat(thread.isVirtual()).isTrue();
    assertThat(thread.getName()).startsWith("Strands-Framework-Virtual-");
    thread.start();
    latch.await();
  }

  @Test
  public void brokenFactory_throwsActionableUnsupportedOperationException() {
    VirtualThreadSupport.Factory broken = new VirtualThreadSupport.BrokenFactory();
    Executor pool = ForkJoinPool.commonPool();
    UnsupportedOperationException ex =
        assertThrows(UnsupportedOperationException.class, () -> broken.create(pool));

    assertThat(ex).hasMessageThat().contains("--add-opens java.base/java.lang=ALL-UNNAMED");
    assertThat(ex).hasMessageThat().contains("allowUnsafeConcurrentExecution=true");
  }

  @Test
  public void brokenFactory_withCause_throwsUnsupportedOperationExceptionChainingCause() {
    Throwable cause = new NoSuchMethodException("VirtualThreadBuilder.<init>");
    VirtualThreadSupport.Factory broken = new VirtualThreadSupport.BrokenFactory(cause);
    Executor pool = ForkJoinPool.commonPool();
    UnsupportedOperationException ex =
        assertThrows(UnsupportedOperationException.class, () -> broken.create(pool));

    assertThat(ex).hasCauseThat().isSameInstanceAs(cause);
    assertThat(ex).hasMessageThat().contains("Failed to reflectively configure");
    assertThat(ex).hasMessageThat().contains("allowUnsafeConcurrentExecution=true");
  }

  @Test
  public void unsafeFactory_createsVirtualThreadWithoutCustomScheduler() throws Exception {
    Thread.Builder.OfVirtual builder =
        new VirtualThreadSupport.UnsafeFactory().create(ForkJoinPool.commonPool());
    CountDownLatch latch = new CountDownLatch(1);
    Thread thread = builder.start(latch::countDown);

    assertThat(thread.isVirtual()).isTrue();
    latch.await();
  }

  @Test
  public void isJavaLangOpen_withoutAddOpens_returnsFalse() {
    assertThat(VirtualThreadSupport.isJavaLangOpen()).isFalse();
  }

  @Test
  public void reflectionFactory_whenJavaLangNotOpen_returnsBrokenFactory() {
    assertThat(VirtualThreadSupport.isJavaLangOpen()).isFalse();
    VirtualThreadSupport.Factory factory = VirtualThreadSupport.ReflectionFactory.create();
    assertThat(factory).isInstanceOf(VirtualThreadSupport.BrokenFactory.class);
  }

  @Test
  public void unsafeFactory_allowsConcurrentExecutionWithoutSequentialBlocking() throws Exception {
    ThreadFactory factory =
        new VirtualThreadSupport.UnsafeFactory()
            .create(ForkJoinPool.commonPool())
            .name("test-unsafe-", 1L)
            .factory();

    CountDownLatch task1Started = new CountDownLatch(1);
    CountDownLatch task2Done = new CountDownLatch(1);
    CountDownLatch task1Done = new CountDownLatch(1);

    // Under the single-execution guarantee, task 1 waiting for task 2 would deadlock,
    // because task 2 could never execute while task 1 occupies the sequential carrier.
    factory
        .newThread(
            () -> {
              task1Started.countDown();
              try {
                task2Done.await();
                task1Done.countDown();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            })
        .start();

    task1Started.await();

    factory.newThread(task2Done::countDown).start();

    boolean completed = task1Done.await(5, SECONDS);
    assertThat(completed).isTrue();
  }
}
