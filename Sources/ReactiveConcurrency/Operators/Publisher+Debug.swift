// MARK: - print

extension Publisher {
    // Logs subscription, values, completion, and cancellation to stdout with an optional prefix.
    public func print(_ prefix: String = "") -> Publisher<Output, Failure> {
        let pfx = prefix.isEmpty ? "" : "\(prefix): "
        return handleEvents(
            receiveSubscription: { Swift.print("\(pfx)receive subscription") },
            receiveOutput: { Swift.print("\(pfx)receive value: (\($0))") },
            receiveCompletion: {
                switch $0 {
                case .finished: Swift.print("\(pfx)receive finished")
                case .failure(let e): Swift.print("\(pfx)receive error: (\(e))")
                }
            },
            receiveCancel: { Swift.print("\(pfx)receive cancel") }
        )
    }
}

// MARK: - assertNoFailure

extension Publisher {
    // Converts to an infallible publisher by asserting failures never arrive.
    // In debug builds, triggers assertionFailure if a failure is received.
    // In release builds, silently finishes instead.
    public func assertNoFailure(
        _ message: @autoclosure @escaping @Sendable () -> String = "",
        file: StaticString = #file,
        line: UInt = #line
    ) -> Publisher<Output, Never> {
        let selfFactory = _stream.factory
        return Publisher<Output, Never>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<Output, Never>> { raw in
                let task = Task {
                    for await result in upstream {
                        switch result {
                        case .success(let v):
                            if case .terminated = raw.yield(.success(v)) { return }
                        case .failure(let e):
                            let msg = message()
                            assertionFailure(
                                msg.isEmpty ? "assertNoFailure received: \(e)" : "\(msg): \(e)",
                                file: file,
                                line: line
                            )
                            raw.finish(); return
                        }
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }
}

// MARK: - breakpointOnError / breakpoint

extension Publisher {
    // Raises assertionFailure (debug only) when a failure is received.
    public func breakpointOnError() -> Publisher<Output, Failure> {
        handleEvents(receiveCompletion: {
            if case .failure(let e) = $0 { assertionFailure("breakpointOnError: \(e)") }
        })
    }

    // Raises assertionFailure (debug only) when the provided closure returns true.
    public func breakpoint(
        receiveOutput: (@Sendable (Output) -> Bool)? = nil,
        receiveCompletion: (@Sendable (Subscribers.Completion<Failure>) -> Bool)? = nil
    ) -> Publisher<Output, Failure> {
        handleEvents(
            receiveOutput: { if receiveOutput?($0) == true { assertionFailure("breakpoint: output") } },
            receiveCompletion: { if receiveCompletion?($0) == true { assertionFailure("breakpoint: completion") } }
        )
    }
}
