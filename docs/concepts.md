# Strands Concepts and Primitives

## Execution Environments

An `Environment` controls the core behavior of the Strands framework within a
given JVM. It attaches monitoring hooks via an `EventListener` and dictates how
a `ContextPropagationOperator` propagates `ThreadLocal` context data.

By default, the `Strands.concurrent(...)` boundaries dynamically search for a
registered `Environment` using Java's `ServiceLoader`.

If you need specialized orchestration logic, you can provide an explicit
`Environment` when starting your root tree:

```java
Environment customEnv = ...;
ListenableFuture<String> result = Strands.concurrent(customEnv, () -> start());
```

## Scopes and Structured Concurrency

Strands relies on **structured concurrency**. This principle states that
concurrent execution flow follows strict lexical scoping rules, identical to how
variables scoped in a block are discarded when the block exits.

In Strands, a `Scope` represents a point in your execution flow where control
splits into separate `Strand` tasks that the caller eventually awaits.

### Creating Scopes

`Strands.concurrent(Task)` automatically creates a scope for you to execute a
task with a single-execution guarantee:

```java
ListenableFuture<String> result = Strands.concurrent(() -> {
  // Executing inside a Strands scope.
  // Everything inside here shares the single-execution guarantee.
  return "success";
});
```

You may create *nested* scopes within an existing scope using
`Strands.scope(Task)` to establish distinct lifecycle boundaries, while still
within the bounds of the parent scope's single-execution guarantee:

```java
ListenableFuture<String> result = Strands.concurrent(() -> {
  // Executing inside a Strands scope.
  // Everything inside here shares the single-execution guarantee.
  String a = Strands.scope(() -> {
    // Creates a nested execution scope. If this task exits, Strands interrupts child strands.
    return fetchInitialData();
  });
  String b = Strands.scope(() -> {
    // Executes sequentially after the previous scope completes; scopes are synchronous.
    return fetchSecondaryData();
  });
  return a + b;
});
```

A scope automatically guarantees its state before it returns. When execution
leaves a block, Strands guarantees that the scope joins or interrupts all
virtual threads launched within that block.

### Context Propagation and Scoped Values

Strands automatically manages its internal `Scope` context so that child strands
and nested scopes inherit the current scope.

For custom application context, such as gRPC `Context` or trace spans, Strands
uses the `ContextPropagationOperator` configured on the `Environment` to
snapshot and restore context across virtual thread boundaries.

> [!NOTE]
> **`ScopedValue` Handling**: Standard Java 25 does not expose a public
> API to snapshot or inherit arbitrary `ScopedValue` bindings across
> non-`StructuredTaskScope` thread boundaries. While Strands automatically
> propagates its own internal `Scope` context to child strands, arbitrary
> user-defined `ScopedValue` bindings established outside Strands are **not
> automatically inherited** by child strands. If your application relies on
> custom `ScopedValue` bindings, you should either re-bind them explicitly using
> `ScopedValue.where(...)` inside your tasks or propagate them via a custom
> `ContextPropagationOperator`.

## The Strand Primitive

A `Strand<T>` represents an asynchronous task that either returns a value of
type `T` or throws an exception. Strands are null-friendly.

### Lifecycle

Strands models a distinct lifecycle, accessible via `strand.state()`:

-   `CREATED`: The task exists, but Strands has not yet allocated a virtual
    thread.
-   `READY`: Strands has allocated a virtual thread, but the task is not yet
    running.
-   `RUNNING`: The virtual thread is actively executing the task.
-   `SUCCEEDED`: The task completed successfully with a return value.
-   `FAILED`: The task failed with an exception.
-   `CANCELLED`: The caller cancelled the strand via `cancel()`.
-   `TIMEOUT`: The strand timed out during an `await` call.
-   `INTERRUPTED`: An interruption stopped the virtual thread.

### Starting and Awaiting Strands

In most cases, you start and schedule a new strand in the current scope using
`Strands.async(Task)`. Wait for the returned `Strand` to compute with
`.await()`:

```java
Strand<Boolean> authCheck = Strands.async(() -> {
  return authenticate(user);
});

// Blocks the current strand until authCheck completes.
// await() throws InterruptedException if the calling thread is interrupted.
boolean isAuthenticated = authCheck.await();
```

You can also pass `java.time.Duration` for a timeout:

```java
// Throws a FailedTaskException if it takes longer than 5 seconds.
boolean result = authCheck.await(Duration.ofSeconds(5));
```

### Safely Handling Results

By default, `.await()` throws `FailedTaskException` if the `Strand` throws an
exception, hits a timeout, or gets cancelled.

If you prefer treating task completions as explicit values without catching task
exceptions, use `.awaitResult()` instead. Note that `.awaitResult()` wraps task
failures and timeouts into `Result.failure()`, but callers must still handle
`InterruptedException` if the calling thread is interrupted:

```java
import com.google.async.strands.Result;

Result<Boolean> result = authCheck.awaitResult();
if (result.isOk()) {
  System.out.println("Success! " + result.value());
} else {
  System.out.println("Computation failed due to " + result.failure());
}
```

The `Result<T>` object provides a standard interface with methods like `.map()`,
`.flatMap()`, `.orElse()`, `.get()`, and `.orElseThrow(Function)` to cleanly
manage values and fallbacks.

## Composition

If you have multiple strands and want to await a combination of them, use
`Strands.compose()` to gather them efficiently:

```java
import java.util.stream.Stream;

// Wait for the first successful strand, canceling remaining candidates:
Strand<String> s1 = Strands.async(() -> requestFromPrimary());
Strand<String> s2 = Strands.async(() -> requestFromSecondary());
Strand<String> fastest = Strands.compose(s1, s2).firstSuccessful();

// Alternatively, wait for all outputs directly:
Strand<String> s3 = Strands.async(() -> fetchMetadata());
Strand<String> s4 = Strands.async(() -> fetchPayload());
Strand<Stream<String>> allStrs = Strands.compose(s3, s4).allSuccessful();
```

The `compose()` method consumes the inputs into a single `Strand`. Depending on
the terminal operation, it either cancels remaining candidates upon early
resolution (`firstSuccessful()`, `allSuccessful()`) or waits for all candidates
to finish (`allCompleted()`).
