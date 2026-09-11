# Strands Gatherers

## Introduction to Gatherers

Java 22 introduced the
[Stream Gatherers API](https://docs.oracle.com/en/java/javase/22/core/stream-gatherers.html),
which enables custom intermediate operations on Java streams.

Strands uses Gatherers to apply asynchronous functions concurrently over
elements of a stream while enforcing structured concurrency bounds.

You create a Gatherer using `Strands.gather()`, which produces a
`FluentGatherer` builder.

> [!IMPORTANT]
> Do not use Strands gatherers with parallel streams (`stream().parallel()`).

## Creating and Configuring Gatherers

Calling `map()` creates an asynchronous concurrent pipeline within your Strands
scope.

### Concurrency and Buffering

Two settings control throughput during concurrent stream operations:

*   `concurrency(int)` limits how many `Strand` instances process elements
    concurrently. Defaults to `8`.
*   `buffer(int)` defines how many pending items can queue before Strands pauses
    the upstream stream. Defaults to `8`.

> [!NOTE]
> The buffer size must be greater than or equal to the concurrency limit
> (`buffer >= concurrency`). Setting `concurrency` higher than `buffer` throws
> an `IllegalStateException`.

```java
import static com.google.async.strands.Strands.gather;

import com.google.async.strands.Result;
import java.time.Duration;

// Within a Strands scope...
List<Result<Profile>> results = userIds.stream()
    .gather(
        gather()
            .concurrency(10) // Process up to 10 queries simultaneously
            .buffer(20)      // Queue up to 20 pending items (must be >= concurrency)
            .timeout(Duration.ofSeconds(5)) // Limit each query to 5 seconds
            .map(userId -> fetchUserProfile(userId))
    )
    .toList();
```

### Response Ordering

When executing operations in concurrent pipelines, operations can complete out
of order. By default, the gatherer preserves encounter order: it buffers
completed elements to emit them in the exact order received from the upstream
stream.

If downstream processing does not require encounter order, call `unordered()` to
emit results as soon as tasks complete:

```java
List<Result<Profile>> results = userIds.stream()
    .gather(Strands.gather().unordered().map(id -> fetch(id)))
    .toList();
```

## Exception Handling

Because stream intermediate operations cannot throw checked exceptions,
`Strands.gather().map()` wraps each evaluated computation in a `Result<T>`
object.

### Fast Failure

If you prefer stream termination over analyzing individual `Result` outputs,
convert the stream directly to output values using `.mapFailFast()`.

This configuration unwraps successful `Result` values, but immediately aborts
the stream and throws a `FailedTaskException` if *any* transformation fails:

```java
List<Profile> profiles = userIds.stream()
    .gather(
        Strands.gather()
            .concurrency(5)
            .mapFailFast(id -> fetch(id))
    )
    .toList(); // Automatically aborts if any ID fetch throws an exception
```

## FlatMapping

For transformations that decompose a single item into multiple stream responses
asynchronously, use `.flatMap()`. Like `map()`, `flatMap()` provides a fail-fast
alternative: `.flatMapFailFast()`.

```java
// fetchFriends maps an ID to a Stream<String> of friend IDs
List<String> allFriends = userIds.stream()
    .gather(Strands.gather().flatMapFailFast(id -> fetchFriends(id)))
    .toList();
```

The flat-map gatherer flattens the resulting asynchronous streams into the
output while enforcing the configured concurrency limit.
