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
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertThrows;

import com.google.async.strands.testing.StrandsRule;
import com.google.async.strands.testing.StrandsRule.InStrand;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@InStrand
@RunWith(JUnit4.class)
@SuppressWarnings("JdkCollectors") // Avoid Guava dependency.
public final class FluentGathererTest {

  @Rule public final StrandsRule strands = StrandsRule.create();

  @Test
  public void map_ordered_succeeds() {
    List<Result<Integer>> results =
        Stream.of(1, 2, 3)
            .gather(Strands.gather().concurrency(2).<Integer, Integer>map(i -> i * 2))
            .collect(toList());

    assertThat(results.stream().map(Result::value)).containsExactly(2, 4, 6);
  }

  @Test
  public void map_unordered_succeeds() {
    // Delays to ensure they finish out of order.
    List<Result<Integer>> results =
        Stream.of(100, 10)
            .gather(
                Strands.gather()
                    .concurrency(2)
                    .unordered()
                    .<Integer, Integer>map(
                        i -> {
                          Thread.sleep(i);
                          return i;
                        }))
            .collect(toList());

    // In unordered mode, 10 should finish and be pushed before 100.
    assertThat(results.stream().map(Result::value)).containsExactly(10, 100);
  }

  @Test
  public void map_ordered_preservesEncounterOrder() {
    List<Result<Integer>> results =
        Stream.of(100, 50, 10)
            .gather(
                Strands.gather()
                    .concurrency(3)
                    .<Integer, Integer>map(
                        i -> {
                          Thread.sleep(i);
                          return i;
                        }))
            .collect(toList());

    assertThat(results.stream().map(Result::value)).containsExactly(100, 50, 10).inOrder();
  }

  @Test
  public void flatMap_ordered_preservesEncounterOrder() {
    List<Result<Integer>> results =
        Stream.of(100, 50, 10)
            .gather(
                Strands.gather()
                    .concurrency(3)
                    .<Integer, Integer>flatMap(
                        i -> {
                          Thread.sleep(i);
                          return Stream.of(i);
                        }))
            .collect(toList());

    assertThat(results.stream().map(Result::value)).containsExactly(100, 50, 10).inOrder();
  }

  @Test
  public void flatMap_ordered_succeeds() {
    List<Result<Integer>> results =
        Stream.of(1, 2)
            .gather(Strands.gather().<Integer, Integer>flatMap(i -> Stream.of(i, i * 10)))
            .collect(toList());

    assertThat(results.stream().map(Result::value)).containsExactly(1, 10, 2, 20).inOrder();
  }

  @Test
  public void map_enforcesConcurrencyLimit() {
    AtomicInteger activeTasks = new AtomicInteger(0);
    AtomicInteger maxActiveTasks = new AtomicInteger(0);
    int limit = 3;

    var _ =
        IntStream.range(0, 20)
            .boxed()
            .gather(
                Strands.gather()
                    .concurrency(limit)
                    .<Integer, Integer>map(
                        i -> {
                          int current = activeTasks.incrementAndGet();
                          maxActiveTasks.updateAndGet(m -> Math.max(m, current));
                          Thread.sleep(10);
                          activeTasks.decrementAndGet();
                          return i;
                        }))
            .collect(toList());

    assertThat(maxActiveTasks.get()).isAtMost(limit);
  }

  @Test
  public void flatMap_enforcesConcurrencyLimit() {
    AtomicInteger activeTasks = new AtomicInteger(0);
    AtomicInteger maxActiveTasks = new AtomicInteger(0);
    int limit = 3;

    var _ =
        IntStream.range(0, 20)
            .boxed()
            .gather(
                Strands.gather()
                    .concurrency(limit)
                    .<Integer, Integer>flatMap(
                        i -> {
                          int current = activeTasks.incrementAndGet();
                          maxActiveTasks.updateAndGet(m -> Math.max(m, current));
                          Thread.sleep(10);
                          activeTasks.decrementAndGet();
                          return Stream.of(i);
                        }))
            .collect(toList());

    assertThat(maxActiveTasks.get()).isAtMost(limit);
  }

  @Test
  public void map_handlesFailuresResiliently() {
    List<Result<Integer>> results =
        Stream.of(1, 0, 2)
            .gather(
                Strands.gather()
                    .concurrency(2)
                    .<Integer, Integer>map(
                        i -> {
                          if (i == 0) {
                            throw new RuntimeException("fail");
                          }
                          return 10 / i;
                        }))
            .collect(toList());

    assertThat(results).hasSize(3);
    assertThat(results.get(1).isFailed()).isTrue();
    assertThat(results.get(1).failure()).hasMessageThat().isEqualTo("fail");
  }

  @Test
  public void map_capturesCheckedExceptions() {
    List<Result<Integer>> results =
        Stream.of("a")
            .gather(
                Strands.gather()
                    .<String, Integer>map(
                        s -> {
                          throw new IOException("checked");
                        }))
            .collect(toList());

    assertThat(results.get(0).isFailed()).isTrue();
    assertThat(results.get(0).failure()).isInstanceOf(IOException.class);
  }

  @Test
  public void map_cancelsPendingTasksOnShortCircuit() {
    AtomicBoolean secondTaskStarted = new AtomicBoolean(false);
    AtomicBoolean secondTaskInterrupted = new AtomicBoolean(false);

    var _ =
        strands.run(
            () ->
                Stream.of(1, 2)
                    .gather(
                        Strands.gather()
                            .concurrency(2)
                            .<Integer, Integer>map(
                                i -> {
                                  if (i == 1) {
                                    return 1;
                                  }
                                  secondTaskStarted.set(true);
                                  try {
                                    Thread.sleep(10000);
                                  } catch (InterruptedException e) {
                                    secondTaskInterrupted.set(true);
                                    throw e;
                                  }
                                  return 2;
                                }))
                    .findFirst());

    assertThat(secondTaskStarted.get()).isTrue();
    assertThat(secondTaskInterrupted.get()).isTrue();
  }

  @Test
  public void map_unordered_cancelsPendingTasksOnShortCircuit() {
    AtomicBoolean longTaskStarted = new AtomicBoolean(false);
    AtomicBoolean longTaskInterrupted = new AtomicBoolean(false);

    var _ =
        strands.run(
            () ->
                Stream.of(10000, 10) // 10000 is long, 10 is short
                    .gather(
                        Strands.gather()
                            .concurrency(2)
                            .unordered()
                            .<Integer, Integer>map(
                                i -> {
                                  if (i == 10) {
                                    Thread.sleep(i);
                                    return i;
                                  }
                                  longTaskStarted.set(true);
                                  try {
                                    Thread.sleep(i);
                                  } catch (InterruptedException e) {
                                    longTaskInterrupted.set(true);
                                    throw e;
                                  }
                                  return i;
                                }))
                    .findFirst());

    assertThat(longTaskStarted.get()).isTrue();
    assertThat(longTaskInterrupted.get()).isTrue();
  }

  @Test
  public void map_withEmptyStream_returnsEmptyStream() {
    List<Result<Object>> results =
        Stream.<Object>empty()
            .gather(Strands.gather().<Object, Object>map(i -> i))
            .collect(toList());

    assertThat(results).isEmpty();
  }

  @Test
  public void map_withTimeout_throwsTimeoutException() {
    var results =
        Stream.of(1)
            .gather(
                Strands.gather()
                    .timeout(Duration.ofMillis(10))
                    .<Integer, Integer>map(
                        i -> {
                          Thread.sleep(10000);
                          return i;
                        }))
            .collect(toList());

    assertThat(results).hasSize(1);
    assertThat(results.get(0).isFailed()).isTrue();
    assertThat(results.get(0).failure()).isInstanceOf(TimeoutException.class);
  }

  @Test
  public void flatMap_withTimeout_throwsTimeoutException() {
    var results =
        Stream.of(1)
            .gather(
                Strands.gather()
                    .timeout(Duration.ofMillis(10))
                    .<Integer, Integer>flatMap(
                        i -> {
                          Thread.sleep(10000);
                          return Stream.of(i);
                        }))
            .collect(toList());

    assertThat(results).hasSize(1);
    assertThat(results.get(0).isFailed()).isTrue();
    assertThat(results.get(0).failure()).isInstanceOf(TimeoutException.class);
  }

  @Test
  public void buffer_withNonPositiveSize_throwsException() {
    FluentGatherer gatherer = Strands.gather();
    assertThrows(IllegalArgumentException.class, () -> gatherer.buffer(0));
    assertThrows(IllegalArgumentException.class, () -> gatherer.buffer(-1));
  }

  @Test
  public void concurrency_withNonPositiveLimit_throwsException() {
    FluentGatherer gatherer = Strands.gather();
    assertThrows(IllegalArgumentException.class, () -> gatherer.concurrency(0));
    assertThrows(IllegalArgumentException.class, () -> gatherer.concurrency(-1));
  }

  @Test
  public void timeout_withNegativeDuration_throwsException() {
    FluentGatherer gatherer = Strands.gather();
    assertThrows(IllegalArgumentException.class, () -> gatherer.timeout(Duration.ofMillis(-1)));
  }

  @Test
  public void map_withConcurrencyGreaterThanBuffer_throwsException() {
    FluentGatherer gatherer = Strands.gather().concurrency(10).buffer(5);
    assertThrows(IllegalStateException.class, () -> gatherer.map(i -> i));
  }

  @Test
  public void flatMap_unordered_succeeds() {
    // Delays to ensure they finish out of order.
    List<Result<Integer>> results =
        Stream.of(100, 10)
            .gather(
                Strands.gather()
                    .concurrency(2)
                    .unordered()
                    .<Integer, Integer>flatMap(
                        i -> {
                          Thread.sleep(i);
                          return Stream.of(i, i + 1);
                        }))
            .collect(toList());

    // In unordered mode, 10 should finish and be pushed before 100.
    assertThat(results.stream().map(Result::value)).containsExactly(10, 11, 100, 101).inOrder();
  }

  @Test
  public void flatMap_handlesFailuresResiliently() {
    List<Result<Integer>> results =
        Stream.of(1, 0, 2)
            .gather(
                Strands.gather()
                    .concurrency(2)
                    .<Integer, Integer>flatMap(
                        i -> {
                          if (i == 0) {
                            throw new RuntimeException("fail");
                          }
                          return Stream.of(10 / i);
                        }))
            .collect(toList());

    assertThat(results).hasSize(3);
    assertThat(results.get(1).isFailed()).isTrue();
    assertThat(results.get(1).failure()).hasMessageThat().isEqualTo("fail");
  }

  @Test
  public void flatMap_capturesCheckedExceptions() {
    List<Result<Integer>> results =
        Stream.of("a")
            .gather(
                Strands.gather()
                    .<String, Integer>flatMap(
                        s -> {
                          throw new IOException("checked");
                        }))
            .collect(toList());

    assertThat(results.get(0).isFailed()).isTrue();
    assertThat(results.get(0).failure()).isInstanceOf(IOException.class);
  }

  @Test
  public void flatMap_multipleElementsPerInput_succeeds() {
    List<Result<Integer>> results =
        Stream.of(1, 10, 100)
            .gather(
                Strands.gather()
                    .concurrency(3)
                    .<Integer, Integer>flatMap(i -> Stream.of(i, i + 1, i + 2)))
            .collect(toList());

    assertThat(results.stream().map(Result::value).collect(toList()))
        .containsExactly(1, 2, 3, 10, 11, 12, 100, 101, 102)
        .inOrder();
  }

  @Test
  public void map_mixedTaskDurations_preservesOrder() {
    List<Result<Integer>> results =
        Stream.of(100, 10, 50)
            .gather(
                Strands.gather()
                    .concurrency(3)
                    .<Integer, Integer>map(
                        i -> {
                          Thread.sleep(i);
                          return i;
                        }))
            .collect(toList());

    assertThat(results.stream().map(Result::value)).containsExactly(100, 10, 50).inOrder();
  }

  @Test
  public void map_withNullElements_succeeds() {
    List<Result<String>> results =
        Stream.of("a", null, "c")
            .gather(Strands.gather().<String, String>map(String::valueOf))
            .collect(toList());

    assertThat(results.stream().map(Result::value)).containsExactly("a", "null", "c").inOrder();
  }

  @Test
  public void map_returningNull_succeeds() {
    List<Result<String>> results =
        Stream.of("a", "b")
            .gather(Strands.gather().<String, String>map(s -> null))
            .collect(toList());

    assertThat(results.get(0).value()).isNull();
    assertThat(results.get(1).value()).isNull();
  }

  @Test
  public void map_withInfiniteStream_shortCircuits() {
    AtomicInteger counter = new AtomicInteger(0);

    List<Result<Integer>> results =
        Stream.iterate(0, i -> i + 1)
            .gather(
                Strands.gather()
                    .concurrency(5)
                    .<Integer, Integer>map(
                        i -> {
                          counter.incrementAndGet();
                          return i;
                        }))
            .limit(2)
            .collect(toList());

    assertThat(results).hasSize(2);
    // It might process a few more due to concurrency/buffer, but shouldn't run forever.
    assertThat(counter.get()).isAtLeast(2);
    assertThat(counter.get()).isAtMost(10); // buffer(8) + concurrency(5) logic
  }

  @Test
  public void map_withLargeStream_succeeds() {
    int count = 1000;

    List<Result<Integer>> results =
        IntStream.range(0, count)
            .boxed()
            .gather(Strands.gather().concurrency(10).buffer(10).<Integer, Integer>map(i -> i))
            .collect(toList());

    assertThat(results).hasSize(count);
    assertThat(results.get(count - 1).value()).isEqualTo(count - 1);
  }

  @Test
  public void map_unordered_withLargeStream_succeeds() {
    int count = 1000;

    List<Result<Integer>> results =
        IntStream.range(0, count)
            .boxed()
            .gather(
                Strands.gather()
                    .unordered()
                    .concurrency(10)
                    .buffer(10)
                    .<Integer, Integer>map(i -> i))
            .collect(toList());

    assertThat(results).hasSize(count);
  }

  @Test
  public void map_unordered_withTimeout_capturesTimeout() {
    List<Result<Integer>> results =
        Stream.of(1)
            .gather(
                Strands.gather()
                    .unordered()
                    .timeout(Duration.ofMillis(10))
                    .<Integer, Integer>map(
                        i -> {
                          Thread.sleep(10000);
                          return i;
                        }))
            .collect(toList());

    assertThat(results).hasSize(1);
    assertThat(results.get(0).isFailed()).isTrue();
    assertThat(results.get(0).failure()).isInstanceOf(TimeoutException.class);
  }

  @Test
  public void flatMap_withEmptyResultStreams_succeeds() {
    List<Result<Integer>> results =
        Stream.of(1, 2, 3)
            .gather(
                Strands.gather()
                    .<Integer, Integer>flatMap(i -> i == 2 ? Stream.empty() : Stream.of(i)))
            .collect(toList());

    assertThat(results.stream().map(Result::value)).containsExactly(1, 3).inOrder();
  }

  @Test
  public void map_unordered_honorsBufferSizeHigherThanConcurrency() throws Exception {
    AtomicInteger totalPulled = new AtomicInteger();
    CountDownLatch latch = new CountDownLatch(1);

    Strand<List<Result<Integer>>> runner =
        Strands.async(
            () ->
                Stream.iterate(0, i -> i + 1)
                    .limit(10)
                    .peek(i -> totalPulled.incrementAndGet())
                    .gather(
                        Strands.gather()
                            .unordered()
                            .buffer(5)
                            .concurrency(1)
                            .map(
                                i -> {
                                  latch.await();
                                  return i;
                                }))
                    .toList());

    // Wait a bit for it to pull.
    Thread.sleep(200);

    // It should have pulled 6 elements: 5 are in the buffer,
    // and the 6th is blocked inside integrate() after being peeked.
    assertThat(totalPulled.get()).isEqualTo(6);

    latch.countDown();
    List<Result<Integer>> results = runner.await();
    assertThat(results).hasSize(10);
    assertThat(totalPulled.get()).isEqualTo(10);
  }

  @Test
  public void map_ordered_honorsBufferSizeHigherThanConcurrency() throws Exception {
    AtomicInteger totalPulled = new AtomicInteger();
    CountDownLatch latch = new CountDownLatch(1);

    Strand<List<Result<Integer>>> runner =
        Strands.async(
            () ->
                Stream.iterate(0, i -> i + 1)
                    .limit(10)
                    .peek(i -> totalPulled.incrementAndGet())
                    .gather(
                        Strands.gather()
                            .buffer(5)
                            .concurrency(1)
                            .map(
                                i -> {
                                  if (i == 0) {
                                    latch.await();
                                  }
                                  return i;
                                }))
                    .toList());

    Thread.sleep(200);

    assertThat(totalPulled.get()).isEqualTo(5);

    latch.countDown();
    List<Result<Integer>> results = runner.await();
    assertThat(results).hasSize(10);
    assertThat(totalPulled.get()).isEqualTo(10);
  }

  @Test
  public void map_failFast_ordered_succeeds() {
    List<Integer> results =
        Stream.of(1, 2, 3)
            .gather(Strands.gather().concurrency(2).<Integer, Integer>mapFailFast(i -> i * 2))
            .collect(toList());

    assertThat(results).containsExactly(2, 4, 6).inOrder();
  }

  @Test
  public void map_failFast_ordered_abortsOnFailure() {
    Exception exception = new RuntimeException("fail");

    RuntimeException result =
        strands.assertFails(
            RuntimeException.class,
            () ->
                Stream.of(1, 0, 2)
                    .gather(
                        Strands.gather()
                            .concurrency(2)
                            .<Integer, Integer>mapFailFast(
                                i -> {
                                  if (i == 0) {
                                    throw exception;
                                  }
                                  return 10 / i;
                                }))
                    .collect(toList()));

    assertThat(result).isSameInstanceAs(exception);
  }

  @Test
  public void map_failFast_ordered_cancelsOtherTasksOnFailure() {
    AtomicBoolean secondTaskStarted = new AtomicBoolean(false);
    AtomicBoolean secondTaskInterrupted = new AtomicBoolean(false);
    Exception exception = new RuntimeException("fail");

    RuntimeException result =
        strands.assertFails(
            RuntimeException.class,
            () ->
                Stream.of(0, 1) // first task fails immediately, second task runs concurrently
                    .gather(
                        Strands.gather()
                            .concurrency(2)
                            .<Integer, Integer>mapFailFast(
                                i -> {
                                  if (i == 0) {
                                    throw exception;
                                  }
                                  secondTaskStarted.set(true);
                                  try {
                                    Thread.sleep(10000);
                                  } catch (InterruptedException e) {
                                    secondTaskInterrupted.set(true);
                                    throw e;
                                  }
                                  return i;
                                }))
                    .collect(toList()));

    assertThat(result).isSameInstanceAs(exception);
    assertThat(secondTaskStarted.get()).isTrue();
    assertThat(secondTaskInterrupted.get()).isTrue();
  }

  @Test
  public void map_failFast_unordered_succeeds() {
    List<Integer> results =
        Stream.of(100, 10)
            .gather(
                Strands.gather()
                    .concurrency(2)
                    .unordered()
                    .<Integer, Integer>mapFailFast(
                        i -> {
                          Thread.sleep(i);
                          return i;
                        }))
            .collect(toList());

    assertThat(results).containsExactly(10, 100);
  }

  @Test
  public void map_failFast_unordered_abortsOnFailure() {
    Exception exception = new RuntimeException("fail");

    RuntimeException result =
        strands.assertFails(
            RuntimeException.class,
            () ->
                Stream.of(10, 100)
                    .gather(
                        Strands.gather()
                            .concurrency(2)
                            .unordered()
                            .<Integer, Integer>mapFailFast(
                                i -> {
                                  if (i == 10) {
                                    throw exception;
                                  }
                                  Thread.sleep(i);
                                  return i;
                                }))
                    .collect(toList()));

    assertThat(result).isSameInstanceAs(exception);
  }

  @Test
  public void map_failFast_unordered_cancelsOtherTasksOnFailure() {
    Exception exception = new RuntimeException("fail");
    AtomicBoolean secondTaskStarted = new AtomicBoolean(false);
    AtomicBoolean secondTaskInterrupted = new AtomicBoolean(false);

    RuntimeException result =
        strands.assertFails(
            RuntimeException.class,
            () ->
                Stream.of(10, 100) // 10 fails immediately, 100 runs concurrently
                    .gather(
                        Strands.gather()
                            .concurrency(2)
                            .unordered()
                            .<Integer, Integer>mapFailFast(
                                i -> {
                                  if (i == 10) {
                                    throw exception;
                                  }
                                  secondTaskStarted.set(true);
                                  try {
                                    Thread.sleep(i);
                                  } catch (InterruptedException e) {
                                    secondTaskInterrupted.set(true);
                                    throw e;
                                  }
                                  return i;
                                }))
                    .collect(toList()));

    assertThat(result).isSameInstanceAs(exception);
    assertThat(secondTaskStarted.get()).isTrue();
    assertThat(secondTaskInterrupted.get()).isTrue();
  }

  @Test
  public void flatMap_failFast_ordered_succeeds() {
    List<Integer> results =
        Stream.of(1, 2)
            .gather(Strands.gather().<Integer, Integer>flatMapFailFast(i -> Stream.of(i, i * 10)))
            .collect(toList());

    assertThat(results).containsExactly(1, 10, 2, 20).inOrder();
  }

  @Test
  public void flatMap_failFast_ordered_abortsOnFailure() {
    Exception exception = new RuntimeException("fail");

    RuntimeException result =
        strands.assertFails(
            RuntimeException.class,
            () ->
                Stream.of(1, 0)
                    .gather(
                        Strands.gather()
                            .concurrency(2)
                            .<Integer, Integer>flatMapFailFast(
                                i -> {
                                  if (i == 0) {
                                    throw exception;
                                  }
                                  return Stream.of(i);
                                }))
                    .collect(toList()));

    assertThat(result).isSameInstanceAs(exception);
  }
}
