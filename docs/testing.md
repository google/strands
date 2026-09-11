# Testing Strands and Structured Concurrency

The Strands framework provides utilities for testing structured concurrency
workflows. Because Strands executes code asynchronously within isolated scopes,
the framework provides a dedicated `StrandsRule` utility to safely manage the
environment, execution contexts, and timeouts.

## `StrandsRule`

`com.google.async.strands.testing.StrandsRule` provides synchronous verification
for Strands. The rule establishes a test execution environment, eliminating
manual runtime configuration.

### Using as a JUnit 4 `@Rule` with `@InStrand` (Recommended)

`StrandsRule` implements JUnit 4's `TestRule`. Add `@Rule public final
StrandsRule strands = StrandsRule.create();` and annotate your test class or
test method with `@InStrand`. The rule automatically executes test methods
inside a Strands scope, eliminating lambda nesting:

```java
import static com.google.common.truth.Truth.assertThat;

import com.google.async.strands.Strand;
import com.google.async.strands.Strands;
import com.google.async.strands.testing.StrandsRule;
import com.google.async.strands.testing.StrandsRule.InStrand;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@InStrand // Opt-in all test methods in this class
@RunWith(JUnit4.class)
public final class MyStrandsTest {

  @Rule public final StrandsRule strands = StrandsRule.create();

  @Test
  public void testMyStrandsTask() throws Exception {
    // Code runs directly within a Strands scope.
    Strand<String> strand = Strands.async(() -> "Expected");
    assertThat(strand.await()).isEqualTo("Expected");
  }
}
```

You can also apply `@InStrand` to individual test methods. Methods without
`@InStrand` run as standard JUnit 4 tests without an active Strands scope,
making it completely safe to mix in tests that check out-of-scope behavior:

```java
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.async.strands.Strand;
import com.google.async.strands.Strands;
import com.google.async.strands.testing.StrandsRule;
import com.google.async.strands.testing.StrandsRule.InStrand;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class StrandsExampleTest {

  @Rule public final StrandsRule strands = StrandsRule.create();

  @Test
  public void outsideScope_throws() {
    // Unannotated: runs outside scope
    assertThrows(IllegalStateException.class, () -> Strands.async(() -> "value"));
  }

  @Test
  @InStrand
  public void insideScope_succeeds() throws Exception {
    // Annotated @InStrand: runs inside Strands scope.
    Strand<String> strand = Strands.async(() -> "value");

    String result = strand.await();

    assertThat(result).isEqualTo("value");
  }
}
```

### Asserting Failures

`StrandsRule` also provides `assertFails` for testing expected failures.
`assertFails(Class<T> exceptionClass, Task<R, ? extends Throwable> task)`
Executes a task expecting it to fail with the specified exception class, blocks
until failure, and returns the caught exception of type `T` for further
assertions. Note that this does not require manually unwrapping any
`FailedTaskException`.

```java
import static com.google.common.truth.Truth.assertThat;

import com.google.async.strands.testing.StrandsRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class StrandsExampleTest {

  private final StrandsRule strands = StrandsRule.create();

  @Test
  public void throwsException_fails() {
    IllegalStateException exception =
        strands.assertFails(IllegalStateException.class, () -> {
          throw new IllegalStateException("error");
        });

    assertThat(exception).hasMessageThat().contains("error");
  }
}
```

### Manual Execution with `run`

`StrandsRule` can be used to execute tasks explicitly with `run` without
installing it as a `@Rule`. `run(Task<T, ?> task)` executes the task
asynchronously and blocks the current thread awaiting success. It automatically
applies a context-aware default timeout:

```java
import static com.google.common.truth.Truth.assertThat;

import com.google.async.strands.testing.StrandsRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class StrandsExampleTest {

  // No @Rule!
  private final StrandsRule strands = StrandsRule.create();

  @Test
  public void manualExecution_succeeds() {
    String result = strands.run(() -> "value");

    assertThat(result).isEqualTo("value");
  }
}
```
