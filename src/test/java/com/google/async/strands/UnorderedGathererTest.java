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

import com.google.async.strands.Gatherers.ResultFlattenPusher;
import com.google.async.strands.Gatherers.ResultIdentityPusher;
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
public final class UnorderedGathererTest {

  @Rule public final StrandsRule strands = StrandsRule.create();

  @Test
  public void succeeds() {
    // Delays to ensure they finish out of order.
    List<Result<Integer>> results =
        Stream.of(100, 10)
            .gather(
                UnorderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 2, Duration.ZERO, false),
                    i -> {
                      Thread.sleep(i);
                      return i;
                    },
                    ResultIdentityPusher.instance()))
            .collect(toList());

    // In unordered mode, 10 should finish and be pushed before 100.
    assertThat(results.stream().map(Result::value)).containsExactly(10, 100);
  }

  @Test
  public void cancelsPendingTasksOnShortCircuit() {
    AtomicBoolean longTaskStarted = new AtomicBoolean(false);
    AtomicBoolean longTaskInterrupted = new AtomicBoolean(false);

    var _ =
        strands.run(
            () ->
                Stream.of(10000, 10) // 10000 is long, 10 is short
                    .gather(
                        UnorderedGatherer.create(
                            new FluentGatherer.Options(Scope.current(), 8, 2, Duration.ZERO, false),
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
                            },
                            ResultIdentityPusher.instance()))
                    .findFirst());

    assertThat(longTaskStarted.get()).isTrue();
    assertThat(longTaskInterrupted.get()).isTrue();
  }

  @Test
  public void flattensResults() {
    // Delays to ensure they finish out of order.
    List<Result<Integer>> results =
        Stream.of(100, 10)
            .gather(
                UnorderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 2, Duration.ZERO, false),
                    i -> {
                      Thread.sleep(i);
                      return Stream.of(i, i + 1);
                    },
                    ResultFlattenPusher.instance()))
            .collect(toList());

    // In unordered mode, 10 should finish and be pushed before 100.
    assertThat(results.stream().map(Result::value)).containsExactly(10, 11, 100, 101).inOrder();
  }

  @Test
  public void enforcesConcurrencyLimit() {
    AtomicInteger activeTasks = new AtomicInteger(0);
    AtomicInteger maxActiveTasks = new AtomicInteger(0);
    int limit = 3;

    var _ =
        IntStream.range(0, 20)
            .boxed()
            .gather(
                UnorderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, limit, Duration.ZERO, false),
                    i -> {
                      int current = activeTasks.incrementAndGet();
                      maxActiveTasks.updateAndGet(m -> Math.max(m, current));
                      Thread.sleep(10);
                      activeTasks.decrementAndGet();
                      return i;
                    },
                    ResultIdentityPusher.instance()))
            .collect(toList());

    assertThat(maxActiveTasks.get()).isAtMost(limit);
  }

  @Test
  public void handlesFailuresResiliently() {
    List<Result<Integer>> results =
        Stream.of(1, 0, 2)
            .gather(
                UnorderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 2, Duration.ZERO, false),
                    i -> {
                      if (i == 0) {
                        throw new RuntimeException("fail");
                      }
                      return 10 / i;
                    },
                    ResultIdentityPusher.instance()))
            .collect(toList());

    assertThat(results).hasSize(3);
    assertThat(results.stream().filter(Result::isFailed)).hasSize(1);
  }

  @Test
  public void capturesCheckedExceptions() {
    List<Result<Integer>> results =
        Stream.of("a")
            .gather(
                UnorderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 8, Duration.ZERO, false),
                    (Transform<String, Integer, IOException>)
                        s -> {
                          throw new IOException("checked");
                        },
                    ResultIdentityPusher.instance()))
            .collect(toList());

    assertThat(results.get(0).isFailed()).isTrue();
    assertThat(results.get(0).failure()).isInstanceOf(IOException.class);
  }

  @Test
  public void withEmptyStream_returnsEmptyStream() {
    List<Result<Object>> results =
        Stream.<Object>empty()
            .gather(
                UnorderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 8, Duration.ZERO, false),
                    i -> i,
                    ResultIdentityPusher.instance()))
            .collect(toList());

    assertThat(results).isEmpty();
  }

  @Test
  public void multipleElementsPerInput_succeeds() {
    List<Result<Integer>> results =
        Stream.of(1, 10, 100)
            .gather(
                UnorderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 3, Duration.ZERO, false),
                    i -> Stream.of(i, i + 1, i + 2),
                    ResultFlattenPusher.instance()))
            .collect(toList());

    assertThat(results.stream().map(Result::value))
        .containsExactly(1, 2, 3, 10, 11, 12, 100, 101, 102);
  }

  @Test
  public void withNullElements_succeeds() {
    List<Result<String>> results =
        Stream.of("a", null, "c")
            .gather(
                UnorderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 8, Duration.ZERO, false),
                    String::valueOf,
                    ResultIdentityPusher.instance()))
            .collect(toList());

    assertThat(results.stream().map(Result::value)).containsExactly("a", "null", "c");
  }

  @Test
  public void returningNull_succeeds() {
    List<Result<String>> results =
        Stream.of("a", "b")
            .gather(
                UnorderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 8, Duration.ZERO, false),
                    s -> (String) null,
                    ResultIdentityPusher.instance()))
            .collect(toList());

    assertThat(results.get(0).value()).isNull();
    assertThat(results.get(1).value()).isNull();
  }

  @Test
  public void withInfiniteStream_shortCircuits() {
    AtomicInteger counter = new AtomicInteger(0);

    List<Result<Integer>> results =
        Stream.iterate(0, i -> i + 1)
            .gather(
                UnorderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 5, Duration.ZERO, false),
                    i -> {
                      counter.incrementAndGet();
                      return i;
                    },
                    ResultIdentityPusher.instance()))
            .limit(2)
            .collect(toList());

    assertThat(results).hasSize(2);
    // It might process a few more due to concurrency/buffer, but shouldn't run forever.
    assertThat(counter.get()).isAtLeast(2);
    assertThat(counter.get()).isAtMost(10); // buffer(8) + concurrency(5) logic
  }

  @Test
  public void emptyResultStreams_succeeds() {
    List<Result<Integer>> results =
        Stream.of(1, 2, 3)
            .gather(
                UnorderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 8, Duration.ZERO, false),
                    i -> i == 2 ? Stream.empty() : Stream.of(i),
                    ResultFlattenPusher.instance()))
            .collect(toList());

    assertThat(results.stream().map(Result::value)).containsExactly(1, 3);
  }

  @Test
  public void withLargeStream_succeeds() {
    int count = 1000;

    List<Result<Integer>> results =
        IntStream.range(0, count)
            .boxed()
            .gather(
                UnorderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 10, 10, Duration.ZERO, false),
                    i -> i,
                    ResultIdentityPusher.instance()))
            .collect(toList());

    assertThat(results).hasSize(count);
  }

  @Test
  public void withTimeout_capturesTimeout() {
    List<Result<Integer>> results =
        Stream.of(1)
            .gather(
                UnorderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 8, Duration.ofMillis(10), false),
                    i -> {
                      Thread.sleep(10000);
                      return i;
                    },
                    ResultIdentityPusher.instance()))
            .collect(toList());

    assertThat(results).hasSize(1);
    assertThat(results.get(0).isFailed()).isTrue();
    assertThat(results.get(0).failure()).isInstanceOf(TimeoutException.class);
  }

  @Test
  public void honorsBufferSizeHigherThanConcurrency() throws Exception {
    AtomicInteger totalPulled = new AtomicInteger();
    CountDownLatch latch = new CountDownLatch(1);

    Strand<List<Result<Integer>>> runner =
        Strands.async(
            () ->
                Stream.iterate(0, i -> i + 1)
                    .limit(10)
                    .peek(i -> totalPulled.incrementAndGet())
                    .gather(
                        UnorderedGatherer.create(
                            new FluentGatherer.Options(Scope.current(), 5, 1, Duration.ZERO, false),
                            i -> {
                              latch.await();
                              return i;
                            },
                            ResultIdentityPusher.instance()))
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
}
