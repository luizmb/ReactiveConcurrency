# Handling Errors

Typed failures that surface as values — recover, retry, replace, and adapt the error channel.

## Overview

A ``Publisher``'s failure type is part of its static type: a `Publisher<Int, MyError>` can only ever
fail with `MyError`. This is the biggest departure from Combine, whose operators erase everything to
`any Error`. Here the error is carried through a typed `Failure` channel and surfaces as a **value**
at the iteration boundary — iteration never throws. Everything below keeps the pure, lazy model from
<doc:CoreConcepts>.

### Errors are values at the boundary

When you iterate a failable publisher you receive the failure as a `Result` element, typed to the
exact `Failure`:

```swift
for await result in failable.results {   // AsyncSequence<Result<Int, MyError>>
    switch result {
    case let .success(value): print(value)
    case let .failure(error): print(error)   // error is MyError, statically — no casting
    }
}
```

When `Failure == Never`, use `.values` for a plain `AsyncSequence<Output>` with no error case at
all — the type system proves failure is impossible.

### catch and tryCatch — recover with another publisher

`catch` replaces a failure with a recovery publisher. Recovering with a `Publisher<_, Never>` makes
the whole stream infallible. `tryCatch` lets the recovery *itself* fail with a (possibly different)
typed error.

```swift
enum MyError: Error { case boom }

let failing = Publisher<Int, MyError> { c in c.yield(1); c.fail(.boom) }

failing
    .catch { _ in Publisher<Int, Never>.just(99) }   // Publisher<Int, Never>  → 1, 99

failing
    .tryCatch { _ throws(OtherError) in try recover() }   // Publisher<Int, OtherError>
```

### replaceError — a plain fallback

`replaceError(with:)` swaps any failure for a single fallback value, then finishes — the result is
infallible (`Failure == Never`).

```swift
failing.replaceError(with: 0)              // Publisher<Int, Never>   — failure → 0 then finish
```

### retry — resubscribe up to N times

`retry(_:)` resubscribes to the upstream on failure, up to N additional attempts. There is **no
infinite-retry overload** by design — you must state a bound, so a persistently failing source can't
spin forever.

```swift
flaky.retry(2)                     // Publisher<Int, MyError> — up to 3 attempts total
```

Because publishers are cold, each retry re-runs the recipe from scratch. Pair with `delay` from
<doc:ControllingTiming> for backoff.

### mapError, setFailureType, assertNoFailure

- **`mapError`** rewrites the failure without touching values — useful to unify sub-error types into
  one domain error.
- **`setFailureType(to:)`** adapts a `Publisher<_, Never>` to a declared failure type so it composes
  with failable publishers. It never actually fails.
- **`assertNoFailure()`** claims failures can't happen: in DEBUG it traps if one arrives, in release
  it finishes silently. Use it only where a failure would be a programming error.

```swift
failing.mapError { OtherError(underlying: $0) }   // Publisher<Int, OtherError>
neverFailing.setFailureType(to: MyError.self)     // Publisher<Int, MyError>
failing.assertNoFailure()                         // Publisher<Int, Never> — traps in DEBUG on failure
```

`mapError` and `setFailureType` also appear in <doc:TransformingValues>; the `try`-prefixed
operators throughout <doc:TransformingValues> and <doc:FilteringValues> are how you *introduce* a
typed failure into an otherwise infallible chain.

## Topics

### Error handling
- ``Publisher/tryCatch(_:)``
- ``Publisher/replaceError(with:)``
- ``Publisher/retry(_:)``
- ``Publisher/mapError(_:)``
- ``Publisher/setFailureType(to:)``
- ``Publisher/assertNoFailure(_:file:line:)``
