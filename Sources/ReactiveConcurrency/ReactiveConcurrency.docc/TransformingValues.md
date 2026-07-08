# Transforming Values

Reshape each value flowing through a pipeline — map, accumulate, collect, and typed-error `try` forms.

## Overview

Transforming operators take the values a ``Publisher`` emits and produce new ones, one step at a
time. They are pure and lazy like everything else (see <doc:CoreConcepts>) — nothing runs until you
subscribe.

### map and key-path map

`map` transforms each value; there are also key-path overloads that project a property (or a tuple
of properties) without a closure.

```swift
[1, 2, 3].publisher            // Publisher<Int, Never>
    .map { $0 * 10 }           // Publisher<Int, Never>    → 10, 20, 30
    .map { "#\($0)" }          // Publisher<String, Never> → "#10", "#20", "#30"

users.publisher                // Publisher<User, Never>
    .map(\.name)               // Publisher<String, Never>
users.publisher
    .map(\.id, \.name)         // Publisher<(UUID, String), Never>
```

### scan, reduce, collect, count

`scan` emits the running accumulation at each step; `reduce` emits only the final fold when the
upstream finishes; `collect()` buffers everything into one array, `collect(_:)` groups by a fixed
count; `count()` reports how many values arrived.

```swift
[1, 2, 3, 4].publisher                 // Publisher<Int, Never>
    .scan(0) { acc, x in acc + x }     // Publisher<Int, Never>   → 1, 3, 6, 10

[1, 2, 3, 4].publisher
    .reduce(0, +)                      // Publisher<Int, Never>   → 10 (on completion)

[1, 2, 3, 4].publisher
    .collect(2)                        // Publisher<[Int], Never> → [1,2], [3,4]

[1, 2, 3, 4].publisher.count()         // Publisher<Int, Never>   → 4
```

For time-windowed collection (`collect(every:)`), see <doc:ControllingTiming>.

### replaceNil and setFailureType

`replaceNil(with:)` swaps `nil` elements for a default. `setFailureType(to:)` adapts a
`Publisher<_, Never>` into a declared failure type so it can be combined with failable publishers
(it never actually fails).

```swift
[1, nil, 3].publisher              // Publisher<Int?, Never>
    .replaceNil(with: 0)           // Publisher<Int, Never>   → 1, 0, 3

[1, 2].publisher
    .setFailureType(to: MyError.self)   // Publisher<Int, MyError>
```

### The try forms — introduce a typed error

Most transforms have a `try`-prefixed sibling whose closure can throw a **typed** error, turning a
`Publisher<_, Never>` into a `Publisher<_, E>`. Crucially the error type is *preserved* (Swift typed
throws), never erased to `any Error`.

```swift
["1", "x"].publisher
    .tryMap { s throws(ParseError) in try parse(s) }   // Publisher<Int, ParseError>
```

Each throwing form also has a **`Result`-returning** overload — handy when your transform already
yields a `Result` (a decoder, a validator) rather than throwing:

```swift
["1", "2"].publisher
    .tryMap { s -> Result<Int, ParseError> in
        Int(s).map(Result.success) ?? .failure(.notANumber)
    }                                                  // Publisher<Int, ParseError>
```

Available throwing/`Result` transforms include `tryMap`, `tryCompactMap`, `tryScan`, and
`tryReduce`. Filtering has its own `try` forms — see <doc:FilteringValues>.

### mapError, encode, decode

`mapError` rewrites the failure channel without touching values. `encode`/`decode` are thin
`tryMap` wrappers that take a `Result`-returning encoder/decoder, so failures stay typed rather than
becoming `any Error`.

```swift
failing.mapError { OtherError(underlying: $0) }        // Publisher<Int, OtherError>

model.publisher
    .encode { value in encodeJSON(value) }             // Result<Data, EncodeError> → Publisher<Data, EncodeError>

dataPublisher                                          // Publisher<Data, Never>
    .decode { data in decodeJSON(data) }               // Result<User, DecodeError> → Publisher<User, DecodeError>
```

`mapError` and `setFailureType` also appear in <doc:HandlingErrors> from the error-channel angle.

## Topics

### Transforming
- ``Publisher/scan(_:_:)``
- ``Publisher/reduce(_:_:)``
- ``Publisher/collect()``
- ``Publisher/count()``
- ``Publisher/replaceNil(with:)``
- ``Publisher/setFailureType(to:)``
- ``Publisher/mapError(_:)``
