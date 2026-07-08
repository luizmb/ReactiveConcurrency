# Controlling Timing

Shape a stream in time — delay, debounce, throttle, time out, and window by the clock.

## Overview

Time-based operators reshape *when* a ``Publisher`` delivers, not *what* it delivers. The defining
difference from Combine: there is **no ambient scheduler**. Every timing operator takes an explicit
`Clock` (from [Hourglass](https://github.com/luizmb/Hourglass)), so time is an injected dependency —
which makes these operators fully deterministic in tests. As always, the pipeline stays pure and
lazy (see <doc:CoreConcepts>).

### Injecting a Clock

Pass any `Clock` you like: `ContinuousClock()` in production, or Hourglass's `ImmediateClock` /
`TestClock` in tests. Nothing reads a global scheduler.

```swift
import ReactiveConcurrency

searchText                                          // Publisher<String, Never>
    .debounce(for: .milliseconds(300), clock: ContinuousClock())
```

```swift
import Hourglass

// In tests, drive time by hand — no sleep, no flakiness.
let clock = TestClock()
let c = subject.eraseToPublisher()
    .debounce(for: .seconds(1), clock: clock)
    .sink { print($0) }

subject.send("a")
await clock.advance(by: .seconds(1))   // now the debounced value is emitted
// ImmediateClock() collapses all delays to zero for the simplest cases.
```

### delay, debounce, throttle

- **`delay`** shifts everything (values *and* completion) later by an interval.
- **`debounce`** emits a value only after the stream has been quiet for the interval; the timer
  resets on each new value. Ideal for search fields.
- **`throttle`** emits at most once per window — the *first* value when `latest: false`, the *most
  recent* when `latest: true`.

```swift
publisher.delay(for: .seconds(1), clock: ContinuousClock())

searchText.debounce(for: .milliseconds(300), clock: ContinuousClock())

scrollOffsets.throttle(for: .seconds(1), clock: ContinuousClock(), latest: true)
```

### timeout

`timeout` fails with a typed error if no value arrives within the interval of subscription or the
last value. The timeout surfaces through the normal typed `Failure` channel — never a thrown
`any Error`. See <doc:HandlingErrors>.

```swift
enum NetworkError: Error { case timedOut }

response
    .timeout(.seconds(2), clock: ContinuousClock(), error: NetworkError.timedOut)
    // Publisher<Data, NetworkError> — fails with .timedOut on silence
```

### measureInterval and time-windowed collect

`measureInterval` replaces each value with the elapsed `Duration` since the previous one.
`collect(every:)` groups values into arrays flushed at the end of each window; `collect(every:orCount:)`
also flushes early when the buffer reaches a count.

```swift
events.measureInterval(using: clock)                            // Publisher<Duration, F> — gaps

events.collect(every: .seconds(1), clock: clock)                // Publisher<[Event], F> — windowed
events.collect(every: .seconds(1), orCount: 50, clock: clock)   // flush on time OR count
```

(For count-only collection with no clock, use `collect(_:)` from <doc:TransformingValues>.)

## Topics

### Timing
- ``Publisher/delay(for:clock:)``
- ``Publisher/debounce(for:clock:)``
- ``Publisher/throttle(for:clock:latest:)``
- ``Publisher/timeout(_:clock:error:)``
- ``Publisher/measureInterval(using:)``
