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
import java.io.IOException;
import java.util.concurrent.CancellationException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class StrandsRuleTest {

  @Rule public final StrandsRule strands = StrandsRule.create();

  @Test
  public void defaultUnannotated_runsOutsideScope() {
    assertThrows(IllegalStateException.class, Scope::current);
  }

  @Test
  @InStrand
  public void methodAnnotatedWithConcurrent_runsInsideScope() throws Exception {
    Scope currentScope = Scope.current();
    assertThat(currentScope).isNotNull();

    Strand<String> strand = Strands.async(() -> "hello from strand");
    assertThat(strand.await()).isEqualTo("hello from strand");
  }

  @Test
  @InStrand
  public void methodAnnotatedWithConcurrent_exceptionsPropagateUnwrapped() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, StrandsRuleTest::throwIllegalArgumentException);

    assertThat(exception).hasMessageThat().isEqualTo("expected exception");
  }

  private static void throwIllegalArgumentException() {
    throw new IllegalArgumentException("expected exception");
  }

  @Test
  public void run_executesTaskInScope() {
    StrandsRule standaloneRule = StrandsRule.create();
    String result = standaloneRule.run(() -> Strands.async(() -> "standalone").await());
    assertThat(result).isEqualTo("standalone");
  }

  @Test
  public void run_propagatesCancellationException() {
    StrandsRule standaloneRule = StrandsRule.create();
    CancellationException thrown =
        assertThrows(
            CancellationException.class,
            () ->
                standaloneRule.run(
                    () -> {
                      throw new CancellationException("cancelled task");
                    }));
    // The propagated exception comes from the cancelled Future, so it is not the instance (nor
    // message) thrown by the task itself.
    assertThat(thrown).isNotNull();
  }

  @Test
  public void assertFails_catchesExpectedException() {
    StrandsRule standaloneRule = StrandsRule.create();
    IOException thrown =
        standaloneRule.assertFails(
            IOException.class,
            () -> {
              throw new IOException("failed task");
            });
    assertThat(thrown).hasMessageThat().isEqualTo("failed task");
  }

  @Test
  public void assertFails_catchesCancellationException() {
    StrandsRule standaloneRule = StrandsRule.create();
    CancellationException thrown =
        standaloneRule.assertFails(
            CancellationException.class,
            () -> {
              throw new CancellationException("cancelled task");
            });
    // The returned exception comes from the cancelled Future, so it is not the instance (nor
    // message) thrown by the task itself.
    assertThat(thrown).isNotNull();
  }

  @Test
  public void assertFails_whenTaskCancelledAndExpectingDifferentException_failsAssertion() {
    StrandsRule standaloneRule = StrandsRule.create();
    AssertionError error =
        assertThrows(
            AssertionError.class,
            () ->
                standaloneRule.assertFails(
                    IOException.class,
                    () -> {
                      throw new CancellationException("cancelled task");
                    }));
    assertThat(error)
        .hasMessageThat()
        .contains("Expected Strand to fail with class java.io.IOException; actually was cancelled");
    assertThat(error).hasCauseThat().isInstanceOf(CancellationException.class);
  }
}
