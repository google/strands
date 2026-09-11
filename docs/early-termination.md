# Early Termination in Strands

A key feature of structured concurrency is **early termination**: when callers
no longer need results or when tasks exceed deadlines, Strands terminates them
promptly to conserve resources. This guide explains how early termination
signals propagate through Strands, how to write responsive tasks, and what
triggers early termination.

## Early Termination via Interrupts

In Strands, early termination relies on standard Java thread interruption
(`Thread.interrupt()`). Like standard Java threads, Strands uses cooperative
cancellation. When terminating a Strand early, Strands interrupts its virtual
thread. The task observes this signal either as an `InterruptedException` from a
blocking call or as a `true` return value from `Thread.interrupted()`.

> [!IMPORTANT]
> Code running inside a `Task` must respond to interruption and terminate
> promptly. Enclosing scopes block until all child Strand threads terminate.
> If a task ignores interruption and continues executing, the scope blocks
> indefinitely, causing high latency or application hangs.

### Responding to Interrupts

If your task performs blocking operations—such as Java NIO, locks, or blocking
queues—those operations unblock immediately and throw an `InterruptedException`.

```java
Strands.async(() -> {
  try {
    // Throws InterruptedException if cancelled or interrupted by scope closure.
    Thread.sleep(Duration.ofSeconds(10));
  } catch (InterruptedException e) {
    // Clean up resources if necessary.
    throw e; // Rethrow to allow Strands to complete the task in the appropriate terminal state (i.e., INTERRUPTED).
  }
  return "success";
});
```

> [!IMPORTANT]
> If you catch an `InterruptedException` and do not rethrow it, always restore
> the thread's interrupt status using `Thread.currentThread().interrupt()`.
> If you rethrow `InterruptedException`, Strands restores the status
> automatically.

If your task performs CPU-bound computation, you must periodically check the
thread's interrupt status to support cooperative early termination:

```java
Strands.async(() -> {
  long sum = 0;
  for (int i = 0; i < Integer.MAX_VALUE; i++) {
    if (Thread.interrupted()) {
      throw new InterruptedException("Computation cancelled");
    }
    sum += compute(i);
  }
  return sum;
});
```

> [!TIP]
> If your task does not need to clean up resources on interruption, allow
> `InterruptedException` to propagate directly without a `try/catch` block.

## Triggers for Early Termination

Depending on the trigger, the final state of a Strand and its failure exception
differ. Three primary triggers cause early termination:

*   **Cancellation**: Transitions a Strand to the `CANCELLED` state in three
    scenarios:
    *   **Short-circuiting**: When using `Strands.compose(...)` (such as
        `firstSuccessful()` or `allSuccessful()`), the composer cancels all
        remaining sibling strands as soon as it determines the final outcome.
    *   **Fail-fast gatherers**: When bulk execution via
        `Strands.gather().mapFailFast(...)` encounters an error, the gatherer
        cancels active work in its buffer.
    *   **Manual cancellation**: When the caller explicitly calls
        `strand.cancel()`.

*   **Timeouts**: Transitions a Strand to the `TIMEOUT` state in two scenarios:
    *   **Awaiting**: When a caller waits on a Strand via
        `Strand.await(Duration)` or `Strand.awaitResult(Duration)` and the
        duration expires.
    *   **Gathering**: When a `FluentGatherer` configured with
        `timeout(Duration)` executes a transform that exceeds the duration.

*   **Interrupts**: Transitions a Strand to the `INTERRUPTED` state in two scenarios:
    *   **Scope closure**: When the task passed to `Strands.concurrent(...)` or
        `Strands.scope(...)` returns or throws, the scope closes, interrupting
        all unresolved child Strands and blocking until they terminate.
    *   **External interruption**: When an external system or thread pool
        interrupts the virtual thread directly.

> [!NOTE]
> `INTERRUPTED` is the default state for early termination, however, Strands
> supports the `TIMEOUT` and `CANCELLED` states and prefers them internally for
> easier failure handling in these common scenarios.
