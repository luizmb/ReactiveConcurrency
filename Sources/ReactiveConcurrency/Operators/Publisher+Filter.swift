extension Publisher {
    public func filter(
        _ predicate: @escaping @Sendable (Output) -> Bool
    ) -> Publisher<Output, Failure> {
        _operator { downstream, upstream in
            for await result in upstream {
                switch result {
                case .success(let value) where predicate(value):
                    if case .terminated = downstream.yield(.success(value)) { return }
                case .success:
                    continue
                case .failure(let error):
                    _ = downstream.yield(.failure(error))
                    downstream.finish(); return
                }
            }
            downstream.finish()
        }
    }
}
