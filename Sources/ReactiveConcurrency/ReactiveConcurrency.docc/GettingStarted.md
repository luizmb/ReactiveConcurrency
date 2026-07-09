# Getting Started

Install ReactiveConcurrency and build your first pipeline.

## Installation

Add the package to your `Package.swift`:

```swift
.package(url: "https://github.com/luizmb/ReactiveConcurrency.git", from: "1.0.0")
```

Then add the products you need:

```swift
.target(name: "MyApp", dependencies: [
    .product(name: "ReactiveConcurrency", package: "ReactiveConcurrency"),
    // optional operator syntax:
    .product(name: "ReactiveConcurrencyOperators", package: "ReactiveConcurrency"),
    // optional monad-transformer surface:
    .product(name: "ReactiveConcurrencyTransformers", package: "ReactiveConcurrency"),
])
```

## Requirements

- **Swift 6.3** toolchain (Xcode 26.5 on Apple platforms).
- **Apple platforms:** macOS 13+, iOS 16+, tvOS 16+, watchOS 9+, visionOS 1+.
- **Also supported (built & tested in CI):** Linux, Windows, and Android via the open-source
  Swift toolchain.
- Distribution is **Swift Package Manager only** — there is no XCFramework.

## Pipelines are values

A pipeline is a pure, lazy ``Publisher`` value — nothing executes until you `sink` or iterate its
`values`. That makes pipelines easy to build, pass around, and test.

```swift
import ReactiveConcurrency

let pipeline = [1, 2, 3, 4, 5].publisher     // Publisher<Int, Never>
    .filter { $0.isMultiple(of: 2) }         // → 2, 4
    .map { $0 * 10 }                         // → 20, 40

// Boundary: execution happens here.
let cancellable = pipeline.sink { print($0) }   // 20, then 40
```

## Consuming as an AsyncSequence

Every publisher exposes `.values`, an `AsyncSequence`, so it composes with `for await`:

```swift
for await value in pipeline.values {
    print(value)   // 20, then 40
}
```

## Deterministic time

Time-based operators are clock-injected (via Hourglass), so you can drive them with a `TestClock`
for fully deterministic tests — no `sleep`, no flakiness.

## Next steps

- ``Publisher`` — the core reactive type
- **ReactiveConcurrencyOperators** — operator syntax for the same surface
- **ReactiveConcurrencyTransformers** — monad-transformer combinators
