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
import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assume.assumeTrue;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.CountDownLatch;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class UnsafeConcurrentExecutionTest {

  @Before
  public void requireUnsafeProperty() {
    assumeTrue(
        "Test requires -D"
            + VirtualThreadSupport.ALLOW_UNSAFE_CONCURRENT_EXECUTION_PROPERTY
            + "=true",
        Boolean.getBoolean(VirtualThreadSupport.ALLOW_UNSAFE_CONCURRENT_EXECUTION_PROPERTY));
  }

  @Test
  public void strandsConcurrent_runsConcurrentlyUnderUnsafeProperty() throws Exception {
    CountDownLatch task1Started = new CountDownLatch(1);
    CountDownLatch task2Done = new CountDownLatch(1);

    ListenableFuture<Void> future =
        Strands.concurrent(
            () -> {
              var _ =
                  async(
                      () -> {
                        task1Started.countDown();
                        task2Done.await(); // Would deadlock under single-execution guarantee!
                        return null;
                      });

              var _ =
                  async(
                      () -> {
                        task1Started.await();
                        task2Done.countDown();
                        return null;
                      });

              return null;
            });

    future.get(5, SECONDS);
    assertThat(future.isDone()).isTrue();
  }
}
