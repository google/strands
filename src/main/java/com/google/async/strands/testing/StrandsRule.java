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

package com.google.async.strands.testing;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.async.strands.Environment;
import com.google.async.strands.Strands;
import com.google.async.strands.Task;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.jspecify.annotations.Nullable;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A helper class and JUnit 4 {@link TestRule} for testing asynchronous tasks in Strands.
 *
 * <p>The rule can be used in two ways:
 *
 * <h3>1. Automatic Scope Binding with {@link Concurrent @StrandsRule.Concurrent}</h3>
 *
 * Annotating a test class or individual test method with {@code @StrandsRule.Concurrent} tells the
 * rule to execute the test body inside an active Strands {@code Scope}. This allows direct calls to
 * {@link Strands#async}, {@link Strands#scope}, and {@link Scope#current} without manual lambda
 * wrapping:
 *
 * <pre>{@code
 * @StrandsRule.Concurrent // Opt-in all tests in this class
 * @RunWith(JUnit4.class)
 * public final class MyStrandsTest {
 *
 *   @Rule public final StrandsRule strands = StrandsRule.create();
 *
 *   @Test
 *   public void testAsyncTask() {
 *     Strand<String> strand = Strands.async(() -> "expected");
 *     assertThat(strand.await()).isEqualTo("expected");
 *   }
 * }
 * }</pre>
 *
 * <p>Test methods without the {@code @Concurrent} annotation run as standard, unannotated JUnit 4
 * tests without an active Strands scope. This allows testing out-of-scope conditions cleanly within
 * the same test suite:
 *
 * <pre>{@code
 * @RunWith(JUnit4.class)
 * public final class MyScopeTest {
 *
 *   @Rule public final StrandsRule strands = StrandsRule.create();
 *
 *   @Test
 *   public void scopeCurrent_outsideScope_throws() {
 *     // Unannotated: runs outside any scope
 *     assertThrows(IllegalStateException.class, Scope::current);
 *   }
 *
 *   @Test
 *   @StrandsRule.Concurrent
 *   public void scopeCurrent_inScope_returnsScope() {
 *     // Annotated @Concurrent: runs inside a Strands scope!
 *     assertThat(Scope.current()).isNotNull();
 *   }
 * }
 * }</pre>
 *
 * <h3>2. Standalone / Block-Level Execution with {@link #run} and {@link #assertFails}</h3>
 *
 * When not using {@code @Concurrent}, or when testing isolated sub-scopes, use {@link #run} or
 * {@link #assertFails} to execute specific tasks:
 *
 * <pre>{@code
 * @RunWith(JUnit4.class)
 * public final class MyBlockTest {
 *
 *   private final StrandsRule strands = StrandsRule.create();
 *
 *   @Test
 *   public void testExplicitBlock() {
 *     String result = strands.run(() -> Strands.async(() -> "value").await());
 *     assertThat(result).isEqualTo("value");
 *   }
 * }
 * }</pre>
 */
public final class StrandsRule implements TestRule {

  /**
   * Indicates that a test method (or all test methods within a test class) should be executed
   * within an active Strands {@code Scope} when using {@link StrandsRule} as a JUnit 4 {@link
   * TestRule @Rule}.
   *
   * <p>Can be applied at either the class level or the method level.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.TYPE})
  public @interface InStrand {}

  /** Creates a {@link StrandsRule} using the default {@link DefaultEnvironment}. */
  public static StrandsRule create() {
    return new StrandsRule(Environment.DEFAULT);
  }

  /**
   * Creates a {@link StrandsRule} configured with a custom {@link Environment}.
   *
   * @param environment the custom environment to use for managing virtual threads and events
   */
  public static StrandsRule create(Environment environment) {
    return new StrandsRule(environment);
  }

  private final Environment environment;

  StrandsRule(Environment environment) {
    this.environment = environment;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    boolean isConcurrent =
        description.getAnnotation(InStrand.class) != null
            || (description.getTestClass() != null
                && description.getTestClass().isAnnotationPresent(InStrand.class));

    if (!isConcurrent) {
      return base;
    }

    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        Task<@Nullable Void, Throwable> task =
            () -> {
              base.evaluate();
              return null;
            };
        run(task);
      }
    };
  }

  /**
   * Executes the specified {@link Task} within a newly created Strands scope, blocking until the
   * task completes or times out.
   *
   * <p>If the task throws an exception, the original exception cause is thrown directly without
   * wrapping.
   *
   * @param <T> the result type of the task
   * @param task the asynchronous task to execute
   * @return the result produced by the task
   * @throws AssertionError if the task times out or fails unexpectedly
   */
  @CanIgnoreReturnValue
  public <T extends @Nullable Object> T run(Task<T, ? extends Throwable> task) {
    return runWithTimeout(task, determineDefaultTimeout());
  }

  /**
   * Executes the specified {@link Task} within a newly created Strands scope, expecting it to fail
   * with an exception of the specified type.
   *
   * @param <T> the expected exception type
   * @param <R> the task return type
   * @param exceptionClass the class of the expected exception
   * @param task the task expected to fail
   * @return the caught exception of type {@code T}
   * @throws AssertionError if the task completes successfully or fails with a different exception
   */
  @CanIgnoreReturnValue
  public <T extends Throwable, R> T assertFails(
      Class<T> exceptionClass, Task<R, ? extends Throwable> task) {
    return assertFailsWithTimeout(exceptionClass, task, determineDefaultTimeout());
  }

  @CanIgnoreReturnValue
  private <T extends @Nullable Object> T runWithTimeout(
      Task<T, ? extends Throwable> task, Duration timeout) {
    try {
      ListenableFuture<T> resultFuture = Strands.concurrent(environment, task);
      return resultFuture.get(timeout.toMillis(), MILLISECONDS);
    } catch (TimeoutException e) {
      throw new AssertionError("Future did not complete within " + timeout.toMillis() + "ms", e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause != null) {
        sneakyThrow(cause);
      }
      throw new AssertionError(
          "Expected Strand to complete successfully. Actually failed with: " + e.getCause(),
          e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Thread was interrupted before Strand completed", e);
    }
  }

  // Safe because T is an unchecked generic type parameter used to rethrow without wrapping.
  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void sneakyThrow(Throwable t) throws T {
    throw (T) t;
  }

  private <T extends Throwable, R> T assertFailsWithTimeout(
      Class<T> exceptionClass, Task<R, ? extends Throwable> task, Duration timeout) {
    try {
      ListenableFuture<R> resultFuture = Strands.concurrent(environment, task);
      resultFuture.get(timeout.toMillis(), MILLISECONDS);
      throw new AssertionError(
          String.format(
              "Expected Strand to throw %s, actually succeeded with value %s",
              exceptionClass, Futures.getDone(resultFuture)));
    } catch (TimeoutException e) {
      throw new AssertionError("Future did not complete within " + timeout.toMillis() + "ms", e);
    } catch (ExecutionException e) {
      Throwable cause = requireNonNull(e.getCause());
      try {
        return exceptionClass.cast(cause);
      } catch (ClassCastException cce) {
        throw new AssertionError(
            String.format(
                "Expected Strand to fail with %s; actually failed with %s", exceptionClass, cause),
            cce);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Thread was interrupted before promise completed", e);
    }
  }

  private Duration determineDefaultTimeout() {
    @Nullable String testTimeout = System.getenv("TEST_TIMEOUT");
    // Somewhat hacky way to detect if we're running within a debugger so that we can artificially
    // increase the timeout without making the user do it in code.
    return Objects.equals(testTimeout, "9999") ? Duration.ofMinutes(30) : Duration.ofSeconds(10);
  }
}
