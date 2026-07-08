# ``ReactiveConcurrency``

@Metadata {
    @DisplayName("ReactiveConcurrency")
    @TitleHeading("Framework")
    @PageColor(purple)
    @Available(macOS, introduced: "13.0")
    @Available(iOS, introduced: "16.0")
    @Available(tvOS, introduced: "16.0")
    @Available(watchOS, introduced: "9.0")
    @Available(visionOS, introduced: "1.0")
    @CallToAction(url: "doc:BuildYourFirstPipeline", purpose: link, label: "Start the tutorial")
}

@Options {
    @TopicsVisualStyle(detailedGrid)
}

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

### Essentials

- <doc:GettingStarted>
- <doc:CoreConcepts>
- ``Publisher``

### Working with Publishers

- <doc:TransformingValues>
- <doc:FilteringValues>
- <doc:CombiningPublishers>
- <doc:ControllingTiming>
- <doc:HandlingErrors>
- <doc:SharingAndMulticasting>
- <doc:ConsumingPublishers>
- <doc:BridgingAsyncSequence>

### Subjects & Cancellation

- <doc:Subjects>
- ``PassthroughSubject``
- ``CurrentValueSubject``
- ``AnySubject``
- ``Subject``
- ``AnyCancellable``
- ``Cancellable``

### Deferred Effects

- <doc:DeferredEffects>
- ``DeferredTask``
- ``DeferredStream``

### Functional Algebra

- <doc:FunctionalAlgebra>
- <doc:MonadTransformers>

### Publisher Types

- ``AnyPublisher``
- ``ConnectablePublisher``
- ``Record``
- ``BufferStrategy``
- ``Subscribers``

### Diagnostics

- ``Diagnostics``
