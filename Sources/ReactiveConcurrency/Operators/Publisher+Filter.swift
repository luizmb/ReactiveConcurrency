// SPDX-License-Identifier: Apache-2.0

public extension Publisher {
    /// Republishes only the elements that satisfy the given predicate.
    /// - Parameter predicate: A closure returning `true` for elements to keep.
    /// - Returns: A publisher that emits the elements passing the predicate.
    func filter(
        _ predicate: @escaping @Sendable (Output) -> Bool
    ) -> Publisher<Output, Failure> {
        _operator { downstream, upstream in
            for await result in upstream {
                switch result {
                case let .success(value) where predicate(value):
                    if case .terminated = downstream.yield(.success(value)) { return }
                case .success:
                    continue
                case let .failure(error):
                    _ = downstream.yield(.failure(error))
                    downstream.finish(); return
                }
            }
            downstream.finish()
        }
    }
}
