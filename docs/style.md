# Strands Style Guide and Best Practices

Structured concurrency simplifies complex concurrent execution when used
correctly. This guide outlines the stylistic expectations when integrating
`Strands` inside your codebase.

## 1. Avoid calling Future.get() directly

Strands manages cancellation propagation and lifecycle tracking for futures.
Calling `Future.get()` directly loses these advantages.

### Avoid

Manually calling `Future.get()` requires explicit handling of
`InterruptedException` and `ExecutionException` and loses structured
cancellation.

```java
try {
  return future.get(10, TimeUnit.SECONDS);
} catch (InterruptedException e) {
  Thread.currentThread().interrupt();
  throw new RuntimeException(e);
} catch (ExecutionException e) {
  throw new RuntimeException(e.getCause());
}
```

### Preferred

If you don't need to handle the failure within the current scope and want any
`FailedTaskException` to propagate up the parent Strand, use `await()` without
a try/catch block:

```java
T value = Strands.async(future).await(Duration.ofSeconds(10));
```

Alternatively, use `Result` for functional error handling:

```java
Result<T> res = Strands.async(future).awaitResult(Duration.ofSeconds(10));
if (res.isFailed()) {
  handleFailure(res.failure());
} else {
  return res.value();
}
```

## 2. Start tasks aggressively

Blocking code easily introduces unintended serialization. When using Strands,
start asynchronous tasks as soon as all inputs are available and avoid managing
completion order manually.

### Avoid

Calling `await()` on a `Strand` immediately blocks the current thread,
potentially serializing tasks that could have run concurrently.

```java
String a = Strands.async(aTask).await();
String b = Strands.async(bTask).await(); // Blocked on the first Strand completing!
return a + b;
```

### Preferred

Instead, create `Strand` instances as soon as their inputs are available, and
`await()` only when you need the results:

```java
Strand<String> a = Strands.async(aTask);
Strand<String> b = Strands.async(bTask);
return a.await() + b.await();
```

If the number of async tasks is larger (but finite), consider using
`BoundedComposer` via `Strands.compose(...)`:

```java
return Strands.compose(tasks.stream().map(Strands::async))
    .allSuccessful()
    .await()
    .collect(Collectors.joining(", "));
```

If task volume threatens resource limits, consider using `FluentGatherer` via
`Strands.gather(...)`:

```java
return inputs.stream()
    .gather(Strands.gather()
        .buffer(10)
        .concurrency(10)
        .unordered()
        .mapFailFast(this::transform))
    .toList();
```

## 3. Avoid calling Strands.concurrent() recursively

`Strands.concurrent(...)` starts an entirely independent root tree of virtual
threads. Avoid recursively calling `Strands.concurrent(...)` within an executing
child task. These calls incur higher overhead than `Strands.scope(...)` or
`Strands.async(...)`. Recursive calls degrade interrupt signal propagation and
introduce thread-safety hazards because nested root trees do not share the
single-execution guarantee.

### Avoid

Calling `Strands.concurrent()` spawns a completely new root execution tree for
*every* user ID! It is heavyweight and voids the single-execution guarantee.

```java
return Strands.concurrent(() -> {
  List<Strand<UserProfile>> strands = userIds.stream()
      .map(id -> Strands.async(Strands.concurrent(() -> fetchProfile(id))))
      .toList();

  List<UserProfile> profiles = new ArrayList<>();
  for (Strand<UserProfile> strand : strands) {
    profiles.add(strand.await());
  }
  return profiles;
});
```

### Preferred

Use `Strands.async(...)` to natively launch sibling virtual threads that
participate in the parent's lifecycle. It is lightweight, and if the parent
fails or times out, these are automatically interrupted.

```java
return Strands.concurrent(() -> {
  List<Strand<UserProfile>> strands = userIds.stream()
      .map(id -> Strands.async(() -> fetchProfile(id)))
      .toList();

  List<UserProfile> profiles = new ArrayList<>();
  for (Strand<UserProfile> strand : strands) {
    profiles.add(strand.await());
  }
  return profiles;
});
```

## 4. Use Strands.scope() for localized cleanup

If you spawn multiple `Strands.async(...)` operations that are part of a
localized sub-routine, they may outlive that routine and continue running longer
than you expect. If your sub-routine might exit early (for example, by throwing
an exception or returning prematurely), ensure you capture and terminate those
lingering operations by wrapping them in a `Strands.scope()`. Consider wrapping
method bodies in their own scope, similar to Kotlin coroutines.

### Avoid

The `auditLogger` and `cache` strands may "escape" the `processTransaction`
method and continue running even if `InvalidTransactionException` is thrown.

```java
public void processTransaction(Transaction tx) throws InvalidTransactionException {
  Strands.async(() -> auditLogger.logStart(tx));
  Strands.async(() -> cache.prefetchData(tx));

  ValidationResult result = validator.validate(tx);
  if (!result.isValid()) {
    throw new InvalidTransactionException();
  }

  // ... continue processing ...
}
```

### Preferred

Using a `Strands.scope(...)` creates a structured concurrency scope. Exiting the
scope early by throwing an exception automatically interrupts and awaits the
termination of all pending strands within the scope.

```java
public void processTransaction(Transaction tx) throws InvalidTransactionException {
  Strands.scope(() -> {
    Strands.async(() -> auditLogger.logStart(tx));
    Strands.async(() -> cache.prefetchData(tx));

    ValidationResult result = validator.validate(tx);
    if (!result.isValid()) {
      throw new InvalidTransactionException();
    }

    // ... continue processing ...

    return null;
  });
}
```
