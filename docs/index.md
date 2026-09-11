# Strands: Structured Concurrency for Java

## Introduction

Strands implements structured concurrency for Java using virtual threads.
Built for Java 25+, it simplifies complex concurrent workflows and shared state.

Traditional asynchronous programming in Java often requires explicitly tracking
task lifetimes, chaining callbacks, or managing complex synchronization. Strands
uses virtual threads to enable sequential-style blocking code. Structured
concurrency ties task lifetimes to the lexical block that created them,
preventing thread leaks and simplifying error propagation.

### The Single-Execution Guarantee

Strands enforces the **single-execution guarantee**. When you
start a concurrent scope, Strands coordinates all asynchronous tasks within that
scope so that **only one strand runs at a time**.

When a Strand performs a blocking operation (such as waiting for an I/O call, a
timer, or another Strand's result), the underlying execution layer yields to
another ready Strand in the same scope.

Because only one strand runs at a time within a single scope, **you can read and
mutate shared data between Strands without synchronization primitives or
locks**.

## Basic Concept

The `Strands.concurrent()` method roots your computation in a bounded context.
Everything executing within that task shares a single structured concurrency
scope. Inside that scope, you spawn individual `Strand` tasks using
`Strands.async()`.

### Code Example

```java
import static com.google.async.strands.Strands.async;
import static com.google.async.strands.Strands.concurrent;

import com.google.async.strands.Strand;
import com.google.common.util.concurrent.ListenableFuture;

// ...

// Start a structured concurrency scope.
// This executes concurrently with your calling code and returns a ListenableFuture.
ListenableFuture<String> workflowResult = concurrent(() -> {

  // Launch two concurrent network requests (Strands)
  Strand<String> userRequest = async(() -> fetchUser(userId));
  Strand<String> prefsRequest = async(() -> fetchPreferences(userId));

  // Await the results of the strands.
  // Calling await() pauses the current strand and yields execution to userRequest.
  String user = userRequest.await();
  String prefs = prefsRequest.await();

  return formatProfile(user, prefs);
});
```

Under structured concurrency, if `fetchUser` throws an exception, the enclosing
task fails, the scope closes, and Strands automatically interrupts the
`fetchPreferences` strand.

## Key Benefits

Strands provides three primary benefits:

-   **Safety and Lifecycle Management**: Sub-tasks never outlast their parent
    scope, preventing virtual thread leaks. An exception in one task cleanly
    shuts down sibling sub-tasks.
-   **Synchronous Style**: Write asynchronous code sequentially. Return values
    and exceptions flow natively instead of through callback chains or
    chained Futures.
-   **Data Race Freedom via Single-Execution**: Strands concurrency allows
    independent flows to be expressed and acted on without the need for forced
    immutability or error-prone locking. For IO-bound services, which are
    most, this achieves the majority of the value of non-serialized execution
    without the drawbacks of manual synchronization.

## Next Steps

Explore the complete guide to mastering Strands:

-   [**Concepts**](concepts.md): Learn core primitives, scoping semantics,
    and lifecycles.
-   [**Failures**](failures.md): Understand exception propagation and the
    `Result` API.
-   [**Early Termination**](early-termination.md): Manage cooperative
    cancellation and timeouts.
-   [**Testing**](testing.md): Explore best practices and utilities for unit
    testing Strands.
-   [**Gatherers**](gatherers.md): Combine results with parallel streaming
    operations.
-   [**Interoperability**](interop.md): Bridge Strands safely with other
    asynchronous frameworks.
-   [**Style Guide**](style.md): Follow stylistic rules and behavioral
    patterns for Strands integration.
-   [**Roadmap**](roadmap.md): Review future directions, planned features, and
    JDK evolution tracking.
