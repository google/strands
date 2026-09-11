# Strands

[![GitHub Release](https://img.shields.io/github/v/release/google/strands)](https://github.com/google/strands/releases/latest)
[![CI](https://github.com/google/strands/actions/workflows/ci.yml/badge.svg)](https://github.com/google/strands/actions/workflows/ci.yml)

Strands provides structured concurrency for Java using virtual threads,
reducing the syntactic overhead of traditional asynchronous frameworks.

> [!NOTE]
> Strands requires Java 25 or higher.

> [!IMPORTANT]
> This is not an officially supported Google product. This project is not
> eligible for the
> [Google Open Source Software Vulnerability Rewards Program](https://bughunters.google.com/open-source-security).

## Overview

Strands simplifies asynchronous Java programming by leveraging virtual threads
to write sequential-style code that executes concurrently. It provides
structured concurrency primitives (such as scopes, tasks, and gatherers) to
manage complex concurrency graphs reliably and safely.

## Key Features

Strands provides the following core capabilities:

-   **Structured Concurrency**: Manage task lifetimes and cancellation within
    well-defined scopes.
-   **Virtual Thread Integration**: Eliminate nested callbacks and Future chains
    using Java 25+ Virtual Threads.
-   **Fluent Gathering and Composition**: Collect, compose, and transform
    results across multiple asynchronous tasks.

## Documentation

See the [docs](docs/) directory for the following guides:

-   [**Overview and Getting Started**](docs/index.md): Introduction, code
    examples, and benefits.
-   [**Concepts**](docs/concepts.md): Scopes, structured concurrency, context
    propagation, and the Strand lifecycle.
-   [**Failures**](docs/failures.md): Fail-fast defaults, `Result<T>` functional
    error handling, and combinators.
-   [**Early Termination**](docs/early-termination.md): Cancellation, timeouts,
    and interruption handling.
-   [**Gatherers**](docs/gatherers.md): Stream gatherer integration and
    concurrent streaming pipelines.
-   [**Interoperability**](docs/interop.md): Bridges to `ListenableFuture`,
    standard `Future`, and functional interfaces.
-   [**Style Guide**](docs/style.md): Idiomatic usage patterns, best practices,
    and anti-patterns.
-   [**Testing**](docs/testing.md): Synchronous testing with `StrandsRule` and
    `@InStrand`.
-   [**Roadmap**](docs/roadmap.md): Planned features, architectural goals, and
    JDK tracking.

## Links

Access project resources and report issues using the following links:

-   [GitHub Repository](https://github.com/google/strands): View the source code
    and release notes.
-   [Issue Tracker](https://github.com/google/strands/issues): Report a defect
    or feature request.

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md) and [CODE_OF_CONDUCT](CODE_OF_CONDUCT.md)
for details on how to contribute to Strands.

See [SECURITY](SECURITY.md) for details on how to report security issues.

## License

The Apache 2.0 License governs Strands. See the [LICENSE](LICENSE) file for
details.
