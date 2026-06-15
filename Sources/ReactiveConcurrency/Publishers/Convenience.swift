// Result.publisher / Optional.publisher conveniences, mirroring Combine.

extension Result where Success: Sendable {
    // .success(v) -> single-value publisher; .failure(e) -> failing publisher.
    public var publisher: ReactiveConcurrency.Publisher<Success, Failure> {
        switch self {
        case .success(let value): .just(value)
        case .failure(let error): .fail(error)
        }
    }
}

extension Optional where Wrapped: Sendable {
    // .some(v) -> single-value publisher; .none -> empty publisher.
    public var publisher: ReactiveConcurrency.Publisher<Wrapped, Never> {
        switch self {
        case .some(let value): .just(value)
        case .none: .empty()
        }
    }
}
