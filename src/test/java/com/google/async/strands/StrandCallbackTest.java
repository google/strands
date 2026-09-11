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

import com.google.async.strands.testing.StrandsRule;
import com.google.async.strands.testing.StrandsRule.InStrand;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@InStrand
@RunWith(JUnit4.class)
public class StrandCallbackTest {

  @Rule public final StrandsRule strands = StrandsRule.create();

  @Test
  public void appendCallback_manyCallbacks_doesNotThrowStackOverflow() throws Exception {
    int count = 100000;
    AtomicInteger counter = new AtomicInteger();
    CountDownLatch inner = new CountDownLatch(1);
    CountDownLatch outer = new CountDownLatch(1);

    // Kick off a strand that will complete after the callbacks are appended to it.
    AbstractStrand<Void> strand =
        (AbstractStrand<Void>)
            Strands.<Void>async(
                () -> {
                  inner.await();
                  return null;
                });
    // Append many callbacks to the strand.
    for (int i = 0; i < count; i++) {
      if (!strand.pushCallback(counter::incrementAndGet)) {
        throw new IllegalStateException("Callback not appended");
      }
    }
    // Append a callback to countDown the outer latch to let the test know the strand has
    // completed.
    if (!strand.pushCallback(outer::countDown)) {
      throw new IllegalStateException("Callback not appended");
    }
    // Count down the inner latch to let the strand complete and execute the callbacks.
    inner.countDown();
    var _ = strand.await();

    if (!outer.await(5, SECONDS)) {
      throw new TimeoutException();
    }

    assertThat(counter.get()).isEqualTo(count);
  }

  @Test
  public void appendCallback_exceptionThrown_subsequentCallbacksExecuted() throws Exception {
    AtomicInteger successCount = new AtomicInteger();
    CountDownLatch inner = new CountDownLatch(1);
    CountDownLatch outer = new CountDownLatch(1);

    AbstractStrand<Void> strand =
        (AbstractStrand<Void>)
            Strands.<Void>async(
                () -> {
                  inner.await();
                  return null;
                });

    if (!strand.pushCallback(successCount::incrementAndGet)) {
      throw new IllegalStateException("Callback not appended");
    }
    if (!strand.pushCallback(
        () -> {
          throw new RuntimeException("Test Exception");
        })) {
      throw new IllegalStateException("Callback not appended");
    }
    if (!strand.pushCallback(
        () -> {
          successCount.incrementAndGet();
          outer.countDown();
        })) {
      throw new IllegalStateException("Callback not appended");
    }

    inner.countDown();
    var _ = strand.await();

    if (!outer.await(5, SECONDS)) {
      throw new TimeoutException();
    }

    assertThat(successCount.get()).isEqualTo(2);
  }
}
