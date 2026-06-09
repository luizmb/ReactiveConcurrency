extension Publisher {
    public func filter(
        _ predicate: @escaping @Sendable (Output) -> Bool
    ) -> Publisher<Output, Failure> {
        _operator { downstream, upstream in
            while let result = await upstream.next() {
                switch result {
                case .success(let value) where predicate(value):
                    if case .terminated = downstream.yield(Result.success(value)) { return }
                case .success:
                    continue
                case .failure(let error):
                    _ = downstream.yield(Result.failure(error))
                    downstream.finish()
                    return
                }
            }
            downstream.finish()
        }
    }
}
