# ``ReactiveConcurrency``

A cross-platform reactive framework built on modern Swift Concurrency and the compiler's `Sendable`
thread-safety guarantees.

## Overview

If you know Combine, you already know most of the surface — ``Publisher``, `sink`, `map`,
`combineLatest`, subjects. But ReactiveConcurrency is **not** a Combine port: pipelines are pure,
lazy values (nothing runs until you `sink` or iterate), everything is `Sendable`, and it runs on
Linux, Windows, and Android as well as Apple platforms.

```swift
import ReactiveConcurrency

// Build a pipeline — nothing runs yet. This is just a value.
let pipeline = [1, 2, 3, 4, 5].publisher     // Publisher<Int, Never>
    .filter { $0.isMultiple(of: 2) }         // → 2, 4
    .map { $0 * 10 }                         // → 20, 40

// Run it. `sink` is the boundary where execution happens.
let cancellable = pipeline.sink { print($0) }   // 20, then 40

// …or consume it as an AsyncSequence:
for await value in pipeline.values {
    print(value)                                // 20, then 40
}
```

The package ships three products: **ReactiveConcurrency** (the named-function core),
**ReactiveConcurrencyOperators** (operator syntax), and **ReactiveConcurrencyTransformers**
(monad-transformer surface).

## Topics

### Getting Started
- <doc:GettingStarted>

### Core
- ``Publisher``
