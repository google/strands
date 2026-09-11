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
tree. If a `FailedTaskException` reaches the root of the tree, Strands unwraps
it and fails the resulting future with the underlying cause.

To call an external library returning a `ListenableFuture` or `Future` within a
Strands context, adapt it using `Strands.async(Future<T>)`:

```java
import static com.google.async.strands.Strands.async;

import com.google.async.strands.Strand;
import com.google.common.util.concurrent.ListenableFuture;

public void processData() throws InterruptedException {
  // Call a method that returns ListenableFuture<Data>.
  ListenableFuture<Data> futureData = legacyClient.fetchDataAsync();

  // Wrap the future as a strand to integrate it into the scope's error
  // and cancellation handling.
  Strand<Data> futureStrand = async(futureData);

  // Standard await() mechanisms apply.
  Data data = futureStrand.await();
}
```

### Cancellation and Wrapping Futures

When you adapt a `Future` using `Strands.async(Future<T>)`, Strands integrates
it into the active scope's lifecycle. If the future has already completed,
Strands adapts it immediately into a completed `Strand` without allocating or
parking a virtual thread.

For a pending future, Strands parks a virtual thread waiting on `future.get()`.
Virtual threads are lightweight, so blocking on `.get()` does not consume an OS
thread.

Wrapping a `Future` provides the following behavior:

*   When the parent scope closes or interrupts the strand, Strands catches
    `InterruptedException` on the parked virtual thread and calls
    `future.cancel(true)`.
*   When the caller explicitly cancels the strand via `strand.cancel()`, Strands
    calls `future.cancel(false)`.
*   When the future completes exceptionally with an `ExecutionException`,
    Strands unwraps the cause and throws a `FailedTaskException` containing the
    underlying exception.

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
