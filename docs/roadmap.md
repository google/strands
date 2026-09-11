# Strands Roadmap

This document outlines the planned directions, architectural goals, and areas of
exploration for Strands.

## Schedulers and Virtual Thread Runtime

The scheduler roadmap focuses on the following milestones:

*   **OpenJDK Pluggable Schedulers**: Monitor and adopt standard OpenJDK
    APIs for custom virtual thread schedulers as they evolve in Project Loom and
    future JDK releases, replacing internal reflective access.
*   **Client-Supplied Schedulers**: Investigate support for user-configured
    carrier schedulers and custom execution strategies.

## Core and Structured Concurrency

Planned improvements to core structured concurrency include the following goals:

*   **Standardized Concurrency Primitives**: Migrate to finalized OpenJDK
    structured concurrency APIs (such as `StructuredTaskScope` and
    `ThreadFlock`) as they exit preview/incubator status.
*   **Uncooperative Task Cancellation**: Investigate strategies for
    isolating or detaching threads that fail to respond to cooperative
    interruption signals (`InterruptedException`).
*   **Configuration and Dependency Injection**: Provide alternatives to
    Java's `ServiceLoader` for environment configuration and binding, such as
    programmatic builders and framework integrations.

## Features and Streaming

Planned feature and streaming integrations include the following areas:

*   **Reactive Streams and Flow Integration**: Support bidirectional
    bridging between Strands and `java.util.concurrent.Flow` or Reactive
    Streams.
*   **Async Framework Interoperability**: Deepen interop with additional async
    paradigms (such as `CompletableFuture`, Kotlin Coroutines, and Project
    Reactor).

## Testing and Benchmarks

Future testing and benchmarking efforts prioritize the following initiatives:

*   **Performance Benchmarking**: Establish comprehensive JMH benchmarks for
    throughput, memory footprint, and latency across varying concurrency bounds
    and carrier thread pool sizes.
*   **Unsafe Mode Testing**: Continue stress testing and auditing Strands
    behavior under concurrent parallel execution
    (`-Dcom.google.async.strands.allowUnsafeConcurrentExecution=true`).
*   **Future JDK Compatibility**: Maintain continuous verification against
    early-access OpenJDK releases (JDK 25+).
