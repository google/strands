# Exception Handling and Failures in Strands

In Strands, structured concurrency ensures that all asynchronous work binds to a
well-defined scope. Consequently, exceptions thrown in a Strand do not silently
disappear. Strands propagates them through `strand.await()` or
`strand.awaitResult()`, allowing callers to catch `FailedTaskException` or
inspect failures functionally using the `Result` API.

This guide explains how failures propagate through strands, how to observe
failure, and how to degrade gracefully when errors occur.

## Fail Fast By Default

Calling `await()` on a `Strand` rethrows any checked or unchecked exception from
the task as a `FailedTaskException`.

```java
Strand<String> strand = Strands.async(() -> {
  throw new IOException("Server error");
});

try {
  // Awaits completion and unwraps the result, throwing if the task failed.
  String value = strand.await();
} catch (FailedTaskException e) {
  // Extract the underlying cause:
  Throwable cause = e.getCause(); // IOException
  System.out.println(cause.getMessage()); // "Server error"
}
```

Similarly, when a Strand times out (`await(Duration)`) or undergoes cooperative
cancellation, `await()` throws a `FailedTaskException` wrapping a
`TimeoutException` or `CancellationException`.

## The `Result` Object for Graceful Degradation

To handle errors functionally without `try/catch` blocks, use `Result<T>`.
A `Result` is a union type representing either a successful value or a failure
exception.

You can receive a `Result` directly by calling `awaitResult()`:

```java
Strand<String> strand = Strands.async(() -> potentiallyFailingTask());

Result<String> result = strand.awaitResult();
if (result.isFailed()) {
  System.out.println("Failed with: " + result.failure());
} else {
  System.out.println("Completed with: " + result.value());
}
```

### Result Combinators

`Result` offers combinator methods for fallback generation and transformations:

*   **`orElse(T defaultValue)`**: Returns a `Result<T>` containing
    `defaultValue` if the original result failed. Overloads accept a failure
    `Class` or `Predicate`.
*   **`orElseGet(Task)`**: Runs a fallback task synchronously if the result
    failed, returning a new `Result<T>`.
*   **`orElseAsync(Task)`**: Runs a fallback task asynchronously on a new
    Strand and returns its `Result<T>`.
*   **`map(Transform)` / `flatMap(Transform)`**: Transforms successful results,
    short-circuiting over failures.
*   **`orElseThrow(...)`**: Throws a custom exception if the result failed.

```java
// Fallback to cache without checking exception type (returns Result<String>)
Result<String> cachedResult = strand.awaitResult().orElse("cached fallback");
String data = cachedResult.value();

// Fallback using another async task specific to a TimeoutException
Result<String> withFallback = strand.awaitResult()
    .orElseAsync(TimeoutException.class, () -> retrySlowTask());
```

## Exceptions in Top-level Scopes (`Strands.concurrent`)

When you create the outermost scope using `Strands.concurrent(...)`, exceptions
propagate up the Strands tree to the root task.

If a `FailedTaskException` reaches the root task, Strands automatically unwraps
it. The `ListenableFuture` returned by `Strands.concurrent(...)` then fails with
the original cause.

## Gatherers and Error Policies

Bulk task execution requires explicit error-handling policies. Strands
integrates with `java.util.stream.Gatherer` through `Strands.gather()`.

The behavior on error depends on the gatherer configuration:

*   **Graceful Degradation (`map` / `flatMap` returning `Result<T>`)**: To
    capture exceptions inside a `Result` object and allow downstream operations
    to handle individual failures, use `map()` or `flatMap()`:

    ```java
    List<Result<String>> results = listOfIds.stream()
        .gather(Strands.gather().concurrency(10).buffer(20).map(id -> fetch(id)))
        .toList();
    ```

*   **Fail Fast (`mapFailFast` / `flatMapFailFast`)**: To abort the entire
    stream and cancel active sibling operations on the first exception, use
    `mapFailFast()` or `flatMapFailFast()`:

    ```java
    try {
      List<String> results = listOfIds.stream()
          .gather(Strands.gather().concurrency(10).buffer(20).mapFailFast(id -> fetch(id)))
          .toList();
    } catch (FailedTaskException e) {
      System.out.println("At least one target failed: " + e.getCause());
    }
    ```

> [!TIP]
> See [Gatherers](gatherers.md) for more details.

## Handling Failures when Composing Multiple Strands

The `BoundedComposer` framework (`Strands.compose(...)`) enforces structured
concurrency by determining overall outcomes and cancelling siblings on failure:

*   `compose(strands).allSuccessful()`: Returns a `Strand<Stream<T>>` that
    cancels remaining candidates and throws immediately on `await()` if any
    strand fails.
*   `compose(strands).firstSuccessful()`: Returns a `Strand<T>` that succeeds if
    any strand succeeds, cancelling the rest. If all fail, it throws the first
    failure encountered and adds subsequent failures as suppressed exceptions.
*   `compose(strands).allCompleted()`: Returns a `Strand<Stream<Result<T>>>`
    that waits for all strands to complete without cancelling siblings, allowing
    manual inspection of successes and failures.
