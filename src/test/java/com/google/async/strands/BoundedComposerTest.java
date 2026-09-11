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

import com.google.async.strands.testing.StrandsRule;
import com.google.async.strands.testing.StrandsRule.InStrand;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@InStrand
@RunWith(JUnit4.class)
@SuppressWarnings("JdkCollectors") // Avoid Guava dependency.
public final class BoundedComposerTest {

  @Rule public final StrandsRule strands = StrandsRule.create();

  @Test
  public void compose_noCandidates_throws() {
    assertThrows(IllegalArgumentException.class, () -> Strands.compose());
  }

  @Test
  public void firstSuccessful_returnsFirstCompleted() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Strand<Integer> s1 =
        Strands.async(
            () -> {
              latch.await();
              return 1;
            });
    Strand<Integer> s2 =
        Strands.async(
            () -> {
              latch.countDown();
              return 2;
            });

    Integer result = Strands.compose(s1, s2).firstSuccessful().await();

    assertThat(result).isEqualTo(2);
  }

  @Test
  public void allSuccessful_returnsAllInOrder() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Strand<Integer> s1 =
        Strands.async(
            () -> {
              latch.await();
              return 1;
            });
    Strand<Integer> s2 =
        Strands.async(
            () -> {
              latch.countDown();
              return 2;
            });

    List<Integer> result =
        Strands.compose(s1, s2).allSuccessful().await().collect(Collectors.toList());

    assertThat(result).containsExactly(1, 2).inOrder();
  }

  @Test
  public void allCompleted_success() throws Exception {
    Strand<Integer> s1 = Strands.async(() -> 1);
    Strand<Integer> s2 = Strands.async(() -> 2);
    Strand<Integer> s3 = Strands.async(() -> 3);
    BoundedComposer<Integer> composer = Strands.compose(s1, s2, s3);

    List<Result<Integer>> results = composer.allCompleted().await().collect(Collectors.toList());

    assertThat(results)
        .containsExactly(Result.ofValue(1), Result.ofValue(2), Result.ofValue(3))
        .inOrder();
  }

  @Test
  public void allCompleted_mixed() throws Exception {
    Exception exception = new Exception("e1");
    Strand<Integer> s1 = Strands.async(() -> 1);
    Strand<Integer> s2 =
        Strands.async(
            () -> {
              throw exception;
            });
    Strand<Integer> s3 = Strands.async(() -> 3);
    BoundedComposer<Integer> composer = Strands.compose(s1, s2, s3);

    List<Result<Integer>> results = composer.allCompleted().await().collect(Collectors.toList());

    assertThat(results)
        .containsExactly(Result.ofValue(1), Result.ofFailure(exception), Result.ofValue(3))
        .inOrder();
  }

  @Test
  public void allCompleted_async() throws Exception {
    RuntimeException exception = new RuntimeException("e2");
    CountDownLatch latch = new CountDownLatch(1);
    Strand<Integer> s1 =
        Strands.async(
            () -> {
              latch.await();
              return 1;
            });
    Strand<Integer> s2 =
        Strands.async(
            () -> {
              latch.countDown();
              throw exception;
            });
    BoundedComposer<Integer> composer = Strands.compose(s1, s2);

    List<Result<Integer>> results = composer.allCompleted().await().collect(Collectors.toList());

    assertThat(results).containsExactly(Result.ofValue(1), Result.ofFailure(exception)).inOrder();
  }

  @Test
  public void allCompleted_alreadyCompletedCandidates() throws Exception {
    Strand<Integer> s1 = Strands.async(() -> 1);
    Strand<Integer> s2 = Strands.async(() -> 2);
    var _ = s1.await();
    var _ = s2.await();

    List<Result<Integer>> results =
        Strands.compose(s1, s2).allCompleted().await().collect(Collectors.toList());

    assertThat(results).containsExactly(Result.ofValue(1), Result.ofValue(2)).inOrder();
  }

  @Test
  public void terminalMethods_throwIfConsumed_firstSuccessful() {
    Strand<Integer> s1 = Strands.async(() -> 1);
    BoundedComposer<Integer> composer = Strands.compose(s1);
    var _ = composer.firstSuccessful();

    IllegalStateException expected =
        assertThrows(IllegalStateException.class, () -> composer.firstSuccessful());

    assertThat(expected).hasMessageThat().contains("has already been consumed");
  }

  @Test
  public void terminalMethods_throwIfConsumed_allSuccessful() {
    Strand<Integer> s1 = Strands.async(() -> 1);
    BoundedComposer<Integer> composer = Strands.compose(s1);
    var _ = composer.allSuccessful();

    IllegalStateException expected =
        assertThrows(IllegalStateException.class, () -> composer.allSuccessful());

    assertThat(expected).hasMessageThat().contains("has already been consumed");
  }

  @Test
  public void terminalMethods_throwIfConsumed_allCompleted() {
    Strand<Integer> s1 = Strands.async(() -> 1);
    BoundedComposer<Integer> composer = Strands.compose(s1);
    var _ = composer.allCompleted();

    IllegalStateException expected =
        assertThrows(IllegalStateException.class, () -> composer.allCompleted());

    assertThat(expected).hasMessageThat().contains("has already been consumed");
  }

  @Test
  public void scopeCheck_firstSuccessful() {
    BoundedComposer<Integer> composer = strands.run(() -> Strands.compose(Strands.async(() -> 1)));

    IllegalStateException expected =
        assertThrows(IllegalStateException.class, () -> composer.firstSuccessful());

    assertThat(expected).hasMessageThat().contains("called from outside the scope");
  }

  @Test
  public void scopeCheck_allSuccessful() {
    BoundedComposer<Integer> composer = strands.run(() -> Strands.compose(Strands.async(() -> 1)));

    IllegalStateException expected =
        assertThrows(IllegalStateException.class, () -> composer.allSuccessful());

    assertThat(expected).hasMessageThat().contains("called from outside the scope");
  }

  @Test
  public void scopeCheck_allCompleted() {
    BoundedComposer<Integer> composer = strands.run(() -> Strands.compose(Strands.async(() -> 1)));

    IllegalStateException expected =
        assertThrows(IllegalStateException.class, () -> composer.allCompleted());

    assertThat(expected).hasMessageThat().contains("called from outside the scope");
  }
}
