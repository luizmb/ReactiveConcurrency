# Filtering Values

Drop, deduplicate, and slice a stream — keep only the values you want.

## Overview

Filtering operators decide which of a ``Publisher``'s values pass through. They never transform a
value's type (that's <doc:TransformingValues>); they only choose whether and when to forward it. As
always, they are pure and lazy — see <doc:CoreConcepts>.

### filter and removeDuplicates

`filter` keeps values matching a predicate. `removeDuplicates()` drops values equal to their
immediate predecessor (consecutive dupes only), with a `by:` overload for custom equality.

```swift
[1, 2, 3, 4].publisher                    // Publisher<Int, Never>
    .filter { $0.isMultiple(of: 2) }      // Publisher<Int, Never>   → 2, 4

[1, 1, 2, 2, 1].publisher                 // Publisher<Int, Never>
    .removeDuplicates()                   // Publisher<Int, Never>   → 1, 2, 1
    // or .removeDuplicates(by: { $0.id == $1.id })
```

### first, last, and their predicated forms

`first()` forwards the first value then completes; `last()` waits for upstream completion and
forwards the final value. Both take a `where:` predicate overload.

```swift
[1, 2, 3, 4, 5].publisher.first()                 // → 1 then finish
[1, 2, 3, 4, 5].publisher.last()                  // → 5 (after upstream finishes)
[1, 2, 3, 4, 5].publisher.first { $0 > 3 }        // → 4 then finish
```

### prefix, drop, and output slicing

`prefix(_:)` takes the leading N values; `prefix(while:)` takes them while a predicate holds.
`dropFirst(_:)` skips the leading N; `drop(while:)` skips while a predicate holds. `output(at:)`
picks a single index; `output(in:)` picks a half-open range.

```swift
[1, 2, 3, 4, 5].publisher.prefix(2)               // → 1, 2 then finish
[1, 2, 3, 4, 5].publisher.prefix { $0 < 3 }       // → 1, 2 then finish
[1, 2, 3, 4, 5].publisher.dropFirst(2)            // → 3, 4, 5
[1, 2, 3, 4, 5].publisher.drop { $0 < 3 }         // → 3, 4, 5
[1, 2, 3, 4, 5].publisher.output(at: 2)           // → 3
[1, 2, 3, 4, 5].publisher.output(in: 1..<3)       // → 2, 3
```

### ignoreOutput, replaceEmpty, replaceNil

`ignoreOutput()` discards every value (keeping only completion/failure). `replaceEmpty(with:)`
substitutes a value if the upstream finishes without emitting. `replaceNil(with:)` swaps `nil`
elements (also covered in <doc:TransformingValues>).

```swift
[1, 2, 3].publisher
    .ignoreOutput()                   // Publisher<Never, Never>  → (no values) then finish

Publisher<Int, Never>.empty()
    .replaceEmpty(with: -1)           // Publisher<Int, Never>    → -1

[1, nil, 3].publisher
    .replaceNil(with: 0)              // Publisher<Int, Never>    → 1, 0, 3
```

### try forms — filtering that can fail

`filter`, `first`, `last`, `drop`, and `prefix` each have a `try`-prefixed sibling whose predicate
throws a **typed** error, turning a `Publisher<_, Never>` into a `Publisher<_, E>`. Each also has a
`Result`-returning form. This mirrors the transforming `try` operators.

```swift
["1", "2", "x"].publisher
    .tryFilter { s throws(ParseError) in try isEven(s) }   // Publisher<String, ParseError>
```

See <doc:HandlingErrors> for how those typed failures surface at the boundary.

## Topics

### Filtering
- ``Publisher/filter(_:)``
- ``Publisher/removeDuplicates()``
- ``Publisher/first()``
- ``Publisher/last()``
- ``Publisher/prefix(_:)``
- ``Publisher/dropFirst(_:)``
- ``Publisher/output(at:)``
- ``Publisher/output(in:)``
- ``Publisher/ignoreOutput()``
- ``Publisher/replaceEmpty(with:)``
