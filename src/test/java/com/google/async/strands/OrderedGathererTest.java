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
public final class OrderedGathererTest {

  @Rule public final StrandsRule strands = StrandsRule.create();

  @Test
  public void succeeds() {
    List<Result<Integer>> results =
        Stream.of(1, 2, 3)
            .gather(
                OrderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 2, Duration.ZERO, true),
                    i -> i * 2,
                    ResultIdentityPusher.instance()))
            .collect(toList());

    assertThat(results.stream().map(Result::value)).containsExactly(2, 4, 6);
  }

  @Test
  public void preservesEncounterOrder() {
    List<Result<Integer>> results =
        Stream.of(100, 50, 10)
            .gather(
                OrderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 3, Duration.ZERO, true),
                    i -> {
                      Thread.sleep(i);
                      return i;
                    },
                    ResultIdentityPusher.instance()))
            .collect(toList());

    assertThat(results.stream().map(Result::value)).containsExactly(100, 50, 10).inOrder();
  }

  @Test
  public void flattensResults() {
    List<Result<Integer>> results =
        Stream.of(1, 2)
            .gather(
                OrderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 8, Duration.ZERO, true),
                    i -> Stream.of(i, i * 10),
                    ResultFlattenPusher.instance()))
            .collect(toList());

    assertThat(results.stream().map(Result::value)).containsExactly(1, 10, 2, 20).inOrder();
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
                OrderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, limit, Duration.ZERO, true),
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
                OrderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 2, Duration.ZERO, true),
                    i -> {
                      if (i == 0) {
                        throw new RuntimeException("fail");
                      }
                      return 10 / i;
                    },
                    ResultIdentityPusher.instance()))
            .collect(toList());

    assertThat(results).hasSize(3);
    assertThat(results.get(1).isFailed()).isTrue();
    assertThat(results.get(1).failure()).hasMessageThat().isEqualTo("fail");
  }

  @Test
  public void capturesCheckedExceptions() {
    List<Result<Integer>> results =
        Stream.of("a")
            .gather(
                OrderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 8, Duration.ZERO, true),
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
  public void cancelsPendingTasksOnShortCircuit() {
    AtomicBoolean secondTaskStarted = new AtomicBoolean(false);
    AtomicBoolean secondTaskInterrupted = new AtomicBoolean(false);

    var _ =
        strands.run(
            () ->
                Stream.of(1, 2)
                    .gather(
                        OrderedGatherer.create(
                            new FluentGatherer.Options(Scope.current(), 8, 2, Duration.ZERO, true),
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
                            },
                            ResultIdentityPusher.instance()))
                    .findFirst());

    assertThat(secondTaskStarted.get()).isTrue();
    assertThat(secondTaskInterrupted.get()).isTrue();
  }

  @Test
  public void withEmptyStream_returnsEmptyStream() {
    List<Result<Object>> results =
        Stream.<Object>empty()
            .gather(
                OrderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 8, Duration.ZERO, true),
                    i -> i,
                    ResultIdentityPusher.instance()))
            .collect(toList());

    assertThat(results).isEmpty();
  }

  @Test
  public void withTimeout_throwsTimeoutException() {
    var results =
        Stream.of(1)
            .gather(
                OrderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 8, Duration.ofMillis(10), true),
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
  public void multipleElementsPerInput_succeeds() {
    List<Result<Integer>> results =
        Stream.of(1, 10, 100)
            .gather(
                OrderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 3, Duration.ZERO, true),
                    i -> Stream.of(i, i + 1, i + 2),
                    ResultFlattenPusher.instance()))
            .collect(toList());

    assertThat(results.stream().map(Result::value).collect(toList()))
        .containsExactly(1, 2, 3, 10, 11, 12, 100, 101, 102)
        .inOrder();
  }

  @Test
  public void mixedTaskDurations_preservesOrder() {
    List<Result<Integer>> results =
        Stream.of(100, 10, 50)
            .gather(
                OrderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 3, Duration.ZERO, true),
                    i -> {
                      Thread.sleep(i);
                      return i;
                    },
                    ResultIdentityPusher.instance()))
            .collect(toList());

    assertThat(results.stream().map(Result::value)).containsExactly(100, 10, 50).inOrder();
  }

  @Test
  public void withNullElements_succeeds() {
    List<Result<String>> results =
        Stream.of("a", null, "c")
            .gather(
                OrderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 8, Duration.ZERO, true),
                    String::valueOf,
                    ResultIdentityPusher.instance()))
            .collect(toList());

    assertThat(results.stream().map(Result::value)).containsExactly("a", "null", "c").inOrder();
  }

  @Test
  public void returningNull_succeeds() {
    List<Result<String>> results =
        Stream.of("a", "b")
            .gather(
                OrderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 8, Duration.ZERO, true),
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
                OrderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 5, Duration.ZERO, true),
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
  public void withLargeStream_succeeds() {
    int count = 1000;

    List<Result<Integer>> results =
        IntStream.range(0, count)
            .boxed()
            .gather(
                OrderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 10, 10, Duration.ZERO, true),
                    i -> i,
                    ResultIdentityPusher.instance()))
            .collect(toList());

    assertThat(results).hasSize(count);
    assertThat(results.get(count - 1).value()).isEqualTo(count - 1);
  }

  @Test
  public void emptyResultStreams_succeeds() {
    List<Result<Integer>> results =
        Stream.of(1, 2, 3)
            .gather(
                OrderedGatherer.create(
                    new FluentGatherer.Options(Scope.current(), 8, 8, Duration.ZERO, true),
                    i -> i == 2 ? Stream.empty() : Stream.of(i),
                    ResultFlattenPusher.instance()))
            .collect(toList());

    assertThat(results.stream().map(Result::value)).containsExactly(1, 3).inOrder();
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
                        OrderedGatherer.create(
                            new FluentGatherer.Options(Scope.current(), 5, 1, Duration.ZERO, true),
                            i -> {
                              if (i == 0) {
                                latch.await();
                              }
                              return i;
                            },
                            ResultIdentityPusher.instance()))
                    .toList());

    Thread.sleep(200);

    assertThat(totalPulled.get()).isEqualTo(5);

    latch.countDown();
    List<Result<Integer>> results = runner.await();
    assertThat(results).hasSize(10);
    assertThat(totalPulled.get()).isEqualTo(10);
  }
}
