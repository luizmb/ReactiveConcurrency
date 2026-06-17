import ReactiveConcurrency

// PublisherTArray: outer = Publisher, inner = Array
// Type: Publisher<[A], F>

// flatMapT: for each emitted [A], apply fn to every element and concatenate all resulting [B]
// arrays into one [B] (matching the ListT concat). Sequential via flatMap(maxPublishers: 1)
// preserves emission order; each element's fn output is folded with reduce + zip.
public func flatMapTPublisherArray<A: Sendable, B: Sendable, F: Error>(
    _ publisher: Publisher<[A], F>,
    _ fn: @escaping @Sendable (A) -> Publisher<[B], F>
) -> Publisher<[B], F> {
    publisher.flatMap(maxPublishers: 1) { (arrA: [A]) -> Publisher<[B], F> in
        arrA.reduce(Publisher<[B], F>.just([])) { acc, a in
            let flattened = fn(a).reduce([B]()) { $0 + $1 }
            return acc.zip(flattened).map { $0.0 + $0.1 }
        }
    }
}

public func bindTPublisherArray<A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> Publisher<[B], F>
) -> @Sendable (Publisher<[A], F>) -> Publisher<[B], F> {
    { @Sendable publisher in flatMapTPublisherArray(publisher, fn) }
}
