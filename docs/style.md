# Strands Style Guide and Best Practices

Because Strands runs on Java Virtual Threads with a single-execution guarantee
per root scope, many habits carried over from callback frameworks
(`CompletableFuture`, `ListenableFuture`, reactive streams) or raw
multithreaded Java become unnecessary boilerplate—or subtle bugs.

## 1. Establish `Strands.concurrent` once at the entry boundary; never nest it {#avoid-nested-concurrent}

Call `Strands.concurrent(...)` once at the boundary where non-Strands code
enters Strands (returning a `ListenableFuture<T>` to the external caller). Never
call `Strands.concurrent(...)` inside an already-active Strands scope: it starts
an independent root tree with its own `SequentialExecutor`, which breaks the
single-execution guarantee and degrades cancellation propagation. Inside an
active scope, use `Strands.scope(...)` or `Strands.async(...)`.

### Avoid

```java
return Strands.concurrent(() -> {
  // Spawns a separate root tree per user ID that runs in parallel with the outer scope!
  List<Strand<UserProfile>> strands =
      userIds.stream()
          .map(id -> Strands.toStrand(Strands.concurrent(() -> fetchProfile(id))))
          .toList();
  return Strands.compose(strands).allSuccessful().await().toList();
});
```

### Preferred

```java
// Establish Strands.concurrent once at the entry boundary; use Strands.async inside:
return Strands.concurrent(() -> {
  List<Strand<UserProfile>> strands =
      userIds.stream().map(id -> Strands.async(() -> fetchProfile(id))).toList();
  return Strands.compose(strands).allSuccessful().await().toList();
});
```

## 2. Accept and return plain values (`T`), not `Strand<T>` (and prefer synchronous blocking APIs) {#return-values-not-strands}

Returning `Strand<T>` from a helper or service method re-introduces wrapper
return-type coloring and forces a child virtual thread to be spawned even when
the caller wants sequential execution. Similarly, accepting `Strand<T>` as a
parameter obscures where suspension happens.

Methods inside a Strands scope should accept unwrapped parameters and return
plain `T`, letting the caller choose sequential vs. concurrent execution. When
performing I/O or calling external services from Strands code, **prefer
synchronous blocking APIs** (such as `HttpClient.send(...)`, JDBC queries,
`java.nio` channels, or blocking RPC clients like gRPC's `BlockingV2Stub`) over
async callback or `Future`-returning variants. Blocking calls on virtual threads
automatically unmount the carrier thread during I/O and yield the scope's
`SequentialExecutor` to other ready strands.

### Avoid

```java
public Strand<UserProfile> loadProfile(UserId id) {
  return Strands.async(() -> fetchFromBackend(id));
}

public Strand<Receipt> chargeUser(Strand<UserProfile> profile, Money amount) {
  return Strands.async(() -> paymentClient.charge(profile.await(), amount));
}
```

### Preferred

```java
public UserProfile loadProfile(UserId id) {
  return fetchFromBackend(id);
}

public Receipt chargeUser(UserProfile profile, Money amount) {
  return paymentClient.charge(profile, amount);
}

// Sequential caller (runs directly on the current virtual thread):
Receipt receipt = chargeUser(loadProfile(id), amount);

// Concurrent caller (spawns parallel Strands inside a scope):
Receipt receipt =
    Strands.scope(
        () -> {
          Strand<UserProfile> profile = Strands.async(() -> loadProfile(id));
          Strand<Inventory> inventory = Strands.async(() -> checkInventory(sku));
          return chargeUser(profile.await(), inventory.await().price());
        });
```

## 3. Encapsulate multi-`Strand` work in `Strands.scope` {#use-strands-scope}

Child strands spawned with `Strands.async(...)` (or `Strands.toStrand(...)`)
register in `Scope.current()`. When a helper method creates multiple `Strand`s,
wrap them in `Strands.scope(...)` so that if one `.await()` throws or the method
returns early, all sibling strands are immediately interrupted and joined before
the method exits—rather than leaking into the caller's outer scope.

Keep two `Scope` invariants in mind:

*   **No "fire-and-forget" `Strands.async`:** When any scope exits (even on a
    normal `return`), `Scope.close()` immediately interrupts all unfinished
    child strands before joining them. Always `.await()` strands whose work must
    complete before the scope returns.
*   **Strict scope affinity:** A `Strand` can only be awaited or composed in the
    exact `Scope` that created it (`Scope.current() == strand.scope()`). Do not
    await an outer `Strand` inside a nested `Strands.scope(...)` or return an
    unawaited `Strand` out of `Strands.scope(...)`.

### Avoid

```java
// Smell 1: No Strands.scope—if profile.await() throws, `orders` keeps running
// in the caller's outer scope after loadUserBundle has already exited!
public UserBundle loadUserBundle(UserId id) throws InterruptedException {
  Strand<Profile> profile = Strands.async(() -> fetchProfile(id));
  Strand<Orders> orders = Strands.async(() -> fetchOrders(id));
  // Smell 2: Unawaited "fire-and-forget" strand will be interrupted as soon as
  // its enclosing scope exits!
  Strands.async(() -> auditLogger.recordAccess(id));
  return UserBundle.of(profile.await(), orders.await());
}
```

### Preferred

```java
public UserBundle loadUserBundle(UserId id) throws InterruptedException {
  return Strands.scope(
      () -> {
        Strand<Profile> profile = Strands.async(() -> fetchProfile(id));
        Strand<Orders> orders = Strands.async(() -> fetchOrders(id));
        Strand<Void> audit = Strands.async(Task.of(() -> auditLogger.recordAccess(id)));
        UserBundle bundle = UserBundle.of(profile.await(), orders.await());
        audit.await();
        return bundle;
      });
}
```

## 4. Reserve `Strands.async` for parallel overlap: start eagerly, await late {#start-tasks-aggressively}

Waiting directly on the calling virtual thread is cheap. Use `Strands.async(...)`
only when you have independent work to overlap in parallel:

*   **Don't wrap single sequential calls in `Strands.async(...).await()`:**
    Running a single blocking call directly on the current virtual thread avoids
    allocating an unnecessary child virtual thread **and avoids introducing
    `.await()` (preventing unnecessary `throws InterruptedException`
    declarations)**.
*   **Don't accidentally serialize independent tasks:** Start all independent
    `Strands.async(...)` tasks before calling `.await()` on any of them.

### Avoid

```java
// Smell 1: Pointless async().await() with no parallel work—spawns a child
// virtual thread AND forces the method to declare `throws InterruptedException`.
String a = Strands.async(() -> backendClient.fetchA(request)).await();

// Smell 2: Awaiting firstTask before starting secondTask runs them sequentially!
String first = Strands.async(firstTask).await();
String second = Strands.async(secondTask).await();
```

### Preferred

```java
// Single sequential call—no child Strand and no `throws InterruptedException`:
String a = backendClient.fetchA(request);

// Parallel overlap—start both Strands eagerly, then await:
Strand<String> first = Strands.async(firstTask);
Strand<String> second = Strands.async(secondTask);
return first.await() + second.await();
```

## 5. Place `try-with-resources` outside `Strands.scope` for multi-`Strand` work {#try-with-resources}

*   **Sequential work (no child `Strand`s):** Use `try-with-resources` normally
    without `Strands.scope`.
*   **Multi-`Strand` work using a resource:** Place `try (...)` **outside**
    `Strands.scope(...)`. Without `Strands.scope(...)`, if the first `.await()`
    throws, Java exits the `try` block and calls `resource.close()` immediately
    while sibling strands in the outer scope are still running and using the
    closed resource.

### Avoid

```java
// BUG: If header.await() throws, cursor.close() runs immediately while `rows`
// is still executing in the caller's scope and reading from `cursor`!
public Report readReport(DatasetId id) throws InterruptedException {
  try (Cursor cursor = openCursor(id)) {
    Strand<Header> header = Strands.async(() -> readHeader(cursor));
    Strand<Rows> rows = Strands.async(() -> readRows(cursor));
    return Report.of(header.await(), rows.await());
  }
}
```

### Preferred

```java
// Multi-Strand: try-with-resources outside Strands.scope so both strands
// terminate before cursor.close() runs:
public Report readReport(DatasetId id) throws InterruptedException {
  try (Cursor cursor = openCursor(id)) {
    return Strands.scope(
        () -> {
          Strand<Header> header = Strands.async(() -> readHeader(cursor));
          Strand<Rows> rows = Strands.async(() -> readRows(cursor));
          return Report.of(header.await(), rows.await());
        });
  }
}

// Sequential (single thread): normal try-with-resources, no Strands.scope needed:
public Header readHeaderOnly(DatasetId id) {
  try (Cursor cursor = openCursor(id)) {
    return readHeader(cursor);
  }
}
```

## 6. Only declare `throws InterruptedException` when calling `.await()`—and propagate it rather than wrapping {#interrupted-exception}

`strand.await()` and `strand.awaitResult()` throw a **checked
`InterruptedException`** if the awaiting thread is interrupted. However, you
should **not** add `throws InterruptedException` to methods as a blanket policy:
methods that only call uninterruptible blocking APIs (such as blocking RPC
clients or `Lock.lock()`), synchronous logic, or `Strands.gather(...)` do not
throw checked `InterruptedException` and should not declare it.

Declare `throws InterruptedException` only when a method calls `.await()`,
`.awaitResult()` (including inside `Strands.scope(...)`, because
`Strands.scope(Task<T, X>) throws X` transparently propagates any checked
exception `X` thrown by its task), or another interruptible blocking API.
Because `Task<T, X>` accepts checked exceptions, let `InterruptedException`
propagate up to the enclosing `Task` boundary (`Strands.concurrent` or
`Strands.async`):

*   Never catch a checked `InterruptedException` and wrap it in an unchecked
    `RuntimeException` just to avoid declaring `throws InterruptedException` on
    a helper method—wrapping it causes Strands to record the task as `FAILED`
    rather than `INTERRUPTED`.
*   Never swallow `InterruptedException` in a broad `catch (Exception e)` block.
*   When writing custom higher-order helpers (e.g., retry or timing wrappers),
    accept `Task<T, X>` or `Transform<I, O, X>` rather than `Supplier` or
    `Function` so callers can propagate checked `InterruptedException`
    naturally.

### Avoid

```java
// Smell: Catching checked InterruptedException and wrapping it in an unchecked
// RuntimeException just to hide "throws InterruptedException" (causes Strands
// to record FAILED instead of INTERRUPTED), or catching broad Exception around
// await() (which swallows scope cancellation!).
public Summary buildSummary(DocId id) {
  try {
    return Strands.scope(
        () -> {
          Strand<Metadata> meta = Strands.async(() -> fetchMetadata(id));
          Strand<Content> body = Strands.async(() -> fetchContent(id));
          return Summary.of(meta.await(), body.await());
        });
  } catch (InterruptedException e) {
    Thread.currentThread().interrupt();
    throw new RuntimeException(e);
  }
}
```

### Preferred

```java
// Propagate InterruptedException when awaiting Strands:
public Summary buildSummary(DocId id) throws InterruptedException {
  return Strands.scope(
      () -> {
        Strand<Metadata> meta = Strands.async(() -> fetchMetadata(id));
        Strand<Content> body = Strands.async(() -> fetchContent(id));
        return Summary.of(meta.await(), body.await());
      });
}
```

## 7. Use `Strands.compose` and `Strands.gather` instead of `for`-looping over `Strand` lists {#compose-and-gather}

Mapping a collection to `List<Strand<T>>` and awaiting each element in a `for`
loop has two flaws: (1) it awaits in rigid index order (`0..N`), so if element
99 fails immediately while element 0 takes 10 seconds, fail-fast cancellation is
delayed for 10 seconds; and (2) `.map(Strands::async).toList()` launches all
tasks with unbounded concurrency. Use `Strands.compose(...)` for small fixed
sets or `Strands.gather(...)` for bounded-concurrency streams (see
[Stream Gatherers](gatherers.md)).

### Avoid

```java
List<Strand<Profile>> strands =
    userIds.stream().map(id -> Strands.async(() -> fetchProfile(id))).toList();
List<Profile> profiles = new ArrayList<>();
for (Strand<Profile> strand : strands) {
  profiles.add(strand.await());
}
```

### Preferred

```java
// Bounded concurrency, backpressure, and immediate fail-fast cancellation:
List<Profile> profiles =
    userIds.stream()
        .gather(Strands.gather().concurrency(10).buffer(20).mapFailFast(this::fetchProfile))
        .toList();
```

## 8. Prefer `awaitResult()` over `await()` when recovering from `Strand` failures {#await-result-error-handling}

Sequential method calls throw their exceptions directly, so use standard Java
`try`/`catch` and flat imperative control flow (`if`, `return`, local
variables). However, `strand.await()` wraps *every* task exception (even
`RuntimeException`) in `FailedTaskException`. When recovering from a failure on
a `Strand`, prefer `strand.awaitResult()` with `Result` recovery methods (see
[Failure Handling](failures.md)) so you never have to unpack
`FailedTaskException` in a `try`/`catch`.

### Avoid

```java
Strand<Badge> badgeStrand = Strands.async(() -> fetchBadge(userId));

// BUG: IOException is wrapped in FailedTaskException, so `catch (IOException)`
// never matches, and catching FailedTaskException requires manual unpacking:
try {
  return badgeStrand.await();
} catch (FailedTaskException e) {
  if (e.getCause() instanceof IOException) {
    return Badge.defaultBadge();
  }
  throw e;
}
```

### Preferred

```java
// For a concurrent Strand, recover cleanly with awaitResult():
Badge badge =
    badgeStrand
        .awaitResult()
        .orElse(IOException.class, Badge.defaultBadge())
        .get();

// For sequential code, use normal imperative control flow and try/catch:
try {
  return fetchBadge(userId);
} catch (IOException e) {
  return Badge.defaultBadge();
}
```

## 9. Bridge `Future`s with `Strands.toStrand` instead of `Future.get` {#avoid-future-get}

While calling `future.get()` (or `Strands.async(() -> future.get())`) is
functional on virtual threads, it does not call `future.cancel(true)` when the
enclosing Strands scope is cancelled, requires manual `ExecutionException`
unwrapping, and triggers static-analysis checks that discourage blocking
`Future.get()` calls. Use `Strands.toStrand(future)` to bridge a `Future` into
the scope's cancellation and error-handling lifecycle.

### Avoid

```java
try {
  return future.get(10, TimeUnit.SECONDS);
} catch (ExecutionException e) {
  throw new RuntimeException(e.getCause());
}
```

### Preferred

```java
T value = Strands.toStrand(future).await(Duration.ofSeconds(10));
```

## 10. Keep scope-local mutable state simple—and respect `await` interleaving boundaries {#shared-mutable-state}

Because at most one strand in a `Strands.concurrent` tree runs at a time,
`ConcurrentHashMap`, `AtomicInteger`, and `synchronized` are unnecessary noise
for state confined to a single scope. However, sibling strands can run whenever
a strand parks at `.await()` or a blocking call—so avoid check-then-act logic or
reading mutable state before `.await()` and assuming it still holds after
`.await()`.

### Avoid

```java
// 1. Unnecessary synchronization for state confined to a single scope:
Map<UserId, Profile> cache = new ConcurrentHashMap<>();

// 2. Check-then-act across an await() boundary: while this strand is parked at
// reserveInventory.await(), a sibling strand can run and spend `remainingBudget`!
if (this.remainingBudget >= order.cost()) {
  Reservation res = reserveInventory.await();
  this.remainingBudget -= order.cost(); // BUG: remainingBudget can go negative!
  placeOrder(res);
}
```

### Preferred

```java
// 1. Plain collections are data-race-free within a single scope:
Map<UserId, Profile> cache = new HashMap<>();

// 2. Complete the await() first, then check and update shared state without an
// intervening suspension point:
Reservation res = reserveInventory.await();
if (this.remainingBudget >= order.cost()) {
  this.remainingBudget -= order.cost();
  placeOrder(res);
}
```
