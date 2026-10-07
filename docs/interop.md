# Interoperability in Strands

When adopting Strands, you often interact with existing asynchronous
primitives, such as `ListenableFuture`, `CompletableFuture`, or callbacks.
Strands bridges in both directions, ensuring you can incrementally adopt
structured concurrency.

## Futures

To execute a block of Strands-based code from an existing context and expose the
outcome as a `ListenableFuture`, use `Strands.concurrent(...)`:

```java
import static com.google.async.strands.Strands.async;
import static com.google.async.strands.Strands.concurrent;

import com.google.async.strands.Strand;
import com.google.common.util.concurrent.ListenableFuture;

public ListenableFuture<Response> handleRequestAsync(Request request) {
  // Initiates a new Strands structured concurrency tree on a virtual thread.
  // The block passed to concurrent() defines the root scope boundary.
  return concurrent(() -> {
    // You are now inside the Strands single-execution context.
    // You can spawn child strands safely here.
    Strand<UserData> userStrand = async(() -> fetchUser(request.getUid()));
    Strand<Session> sessionStrand = async(() -> fetchSession(request.getSid()));

    return Response.create(userStrand.await(), sessionStrand.await());
  });
}
```

The returned `ListenableFuture` represents the completion of the entire Strands
tree:

*   If an exception escapes the root of the tree, Strands unwraps any
    `FailedTaskException` and fails the returned `ListenableFuture` with the
    underlying cause.
*   Cancelling the returned `ListenableFuture` (via either `cancel(true)` or
    `cancel(false)`) interrupts the root virtual thread and cancels all child
    Strands in the scope.
*   An unhandled `CancellationException` escaping the root task (for example,
    from awaiting a cancelled child `Strand`) is treated as a task failure
    rather than a cancelled result `Future`: the returned `ListenableFuture`
    fails with that exception and is only marked `isCancelled() == true` when
    `Future.cancel(boolean)` is called on it directly.

To call an external library returning a `ListenableFuture` or `Future` within a
Strands context, wrap the `Future` in `Strands.toStrand(Future)`:

```java
import static com.google.async.strands.Strands.toStrand;

import com.google.async.strands.Strand;
import com.google.common.util.concurrent.ListenableFuture;

public Data fetchExternalData() throws InterruptedException {
  // Assume legacyClient.fetchDataAsync() returns a ListenableFuture<Data>
  ListenableFuture<Data> futureData = legacyClient.fetchDataAsync();

  // Adapts the Future into a Strand within the current scope.
  Strand<Data> dataStrand = toStrand(futureData);

  // You can now use all standard Strand APIs (await, awaitResult, compose, etc.)
  return dataStrand.await();
}
```

### Cancellation and Exception Handling for Futures

When you pass a `Future` to `Strands.toStrand(Future)`, Strands integrates it
into the active scope's lifecycle. If the future has already completed, Strands
returns an immediate `Strand` without parking a virtual thread. For a pending
future, Strands parks a virtual thread on `future.get()`.

This provides the following behavior:

*   When the parent scope closes, interrupts the strand, or when the caller
    explicitly cancels the `Strand` via `strand.cancel()`, Strands propagates
    cooperative cancellation to the underlying future via `future.cancel(true)`.
*   When the future completes exceptionally with an `ExecutionException`,
    Strands unwraps the cause and fails the `Strand` with the underlying
    exception.

> [!IMPORTANT]
> Strands executes within its own managed virtual threads, but cannot
> propagate trace contexts or thread-locals into the external executor
> managing the nested future. The caller must install the appropriate scope
> context, such as by using context-propagating executors, when initiating the
> future.

## Functional Interfaces (`Runnable` and `Callable`)

Strands primarily uses the `Task<T, X>` functional interface. For existing
callbacks natively implemented as `Runnable` or `Callable`, Strands exposes
utility wrapper methods: `Task.of(Runnable)` and `Task.of(Callable)`.

```java
import static com.google.async.strands.Strands.async;

import com.google.async.strands.Strand;
import com.google.async.strands.Task;
import java.util.concurrent.Callable;

Runnable myRunnable = () -> doWork();
Callable<String> myCallable = () -> computeWork();

Strand<Void> strand1 = Strands.async(Task.of(myRunnable));
Strand<String> strand2 = Strands.async(Task.of(myCallable));
```
