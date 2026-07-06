// SPDX-License-Identifier: Apache-2.0

// Result.publisher / Optional.publisher conveniences, mirroring Combine.

public extension Result where Success: Sendable {
    // .success(v) -> single-value publisher; .failure(e) -> failing publisher.
    var publisher: ReactiveConcurrency.Publisher<Success, Failure> {
        switch self {
        case let .success(value): .just(value)
        case let .failure(error): .fail(error)
        }
    }
}

public extension Optional where Wrapped: Sendable {
    // .some(v) -> single-value publisher; .none -> empty publisher.
    var publisher: ReactiveConcurrency.Publisher<Wrapped, Never> {
        switch self {
        case let .some(value): .just(value)
        case .none: .empty()
        }
    }
}
