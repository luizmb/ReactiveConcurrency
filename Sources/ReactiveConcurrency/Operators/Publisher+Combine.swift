// SPDX-License-Identifier: Apache-2.0

private enum _CombineEvent<A: Sendable, B: Sendable>: Sendable {
    case fromA(A?)
    case fromB(B?)
}

// MARK: - merge

public extension Publisher {
    func merge(with other: Publisher<Output, Failure>) -> Publisher<Output, Failure> {
        let selfFactory = _stream.factory
        let otherFactory = other._stream.factory
        return Publisher<Output, Failure>(DeferredStream {
            // Pre-subscribe both streams synchronously so values sent after merge(...).sink()
            // are not lost while the inner Tasks are starting.
            let selfStream = selfFactory()
            let otherStream = otherFactory()
            return AsyncStream<Result<Output, Failure>> { raw in
                let task = Task {
                    await withTaskGroup(of: Void.self) { group in
                        group.addTask {
                            for await result in selfStream {
                                switch result {
                                case .success:
                                    if case .terminated = raw.yield(result) { return }
                                case .failure:
                                    _ = raw.yield(result); raw.finish(); return
                                }
                            }
                        }
                        group.addTask {
                            for await result in otherStream {
                                switch result {
                                case .success:
                                    if case .terminated = raw.yield(result) { return }
                                case .failure:
                                    _ = raw.yield(result); raw.finish(); return
                                }
                            }
                        }
                        await group.waitForAll()
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }

    func merge(
        with b: Publisher<Output, Failure>,
        _ c: Publisher<Output, Failure>
    ) -> Publisher<Output, Failure> {
        merge(with: b).merge(with: c)
    }

    func merge(
        with b: Publisher<Output, Failure>,
        _ c: Publisher<Output, Failure>,
        _ d: Publisher<Output, Failure>
    ) -> Publisher<Output, Failure> {
        merge(with: b).merge(with: c).merge(with: d)
    }
}

// MARK: - zip

public extension Publisher {
    // swiftlint:disable:next cyclomatic_complexity
    func zip<B: Sendable>(
        _ other: Publisher<B, Failure>
    ) -> Publisher<(Output, B), Failure> {
        let selfFactory = _stream.factory
        let otherFactory = other._stream.factory
        return Publisher<(Output, B), Failure>(DeferredStream {
            // Same FIFO task-group as combineLatest: StreamBox gives each side one shared
            // iterator with exactly one child calling next() at a time. Unlike the old
            // left-driven pull (which only polled `other` after `self` emitted, so a failure
            // or empty-completion on `other` was never observed while `self` was silent —
            // Combine-divergent and a hang source), both sides advance independently here.
            let selfBox = StreamBox<Result<Output, Failure>>(selfFactory())
            let otherBox = StreamBox<Result<B, Failure>>(otherFactory())
            return AsyncStream<Result<(Output, B), Failure>> { raw in
                let task = Task {
                    // Per-side FIFO buffers of values not yet paired.
                    var queueA: [Output] = []
                    var queueB: [B] = []
                    typealias _Event = _CombineEvent<Result<Output, Failure>, Result<B, Failure>>
                    await withTaskGroup(of: _Event.self) { group in
                        group.addTask { .fromA(await selfBox.next()) }
                        group.addTask { .fromB(await otherBox.next()) }

                        var aOpen = true
                        var bOpen = true

                        while aOpen || bOpen, let event = await group.next() {
                            switch event {
                            case let .fromA(.some(result)):
                                switch result {
                                case let .success(a):
                                    queueA.append(a)
                                    if !queueB.isEmpty {
                                        let pair = (queueA.removeFirst(), queueB.removeFirst())
                                        if case .terminated = raw.yield(Result.success(pair)) { return }
                                    }
                                    group.addTask { .fromA(await selfBox.next()) }
                                case let .failure(e):
                                    _ = raw.yield(Result.failure(e)); raw.finish(); return
                                }
                            case let .fromB(.some(result)):
                                switch result {
                                case let .success(b):
                                    queueB.append(b)
                                    if !queueA.isEmpty {
                                        let pair = (queueA.removeFirst(), queueB.removeFirst())
                                        if case .terminated = raw.yield(Result.success(pair)) { return }
                                    }
                                    group.addTask { .fromB(await otherBox.next()) }
                                case let .failure(e):
                                    _ = raw.yield(Result.failure(e)); raw.finish(); return
                                }
                            case .fromA(.none):
                                aOpen = false
                            case .fromB(.none):
                                bOpen = false
                            }
                            // A finished side whose buffer is drained can never contribute another
                            // element, so no future pair can be formed → complete. This matches
                            // Combine (zip completes as soon as either side definitively can't pair)
                            // and resolves the silent-side/empty-side hang.
                            if (!aOpen && queueA.isEmpty) || (!bOpen && queueB.isEmpty) {
                                raw.finish(); return
                            }
                        }
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }

    func zip<B: Sendable, C: Sendable>(
        _ b: Publisher<B, Failure>,
        _ c: Publisher<C, Failure>
    ) -> Publisher<(Output, B, C), Failure> {
        zip(b).zip(c).map { ($0.0.0, $0.0.1, $0.1) }
    }

    func zip<B: Sendable, C: Sendable, D: Sendable>(
        _ b: Publisher<B, Failure>,
        _ c: Publisher<C, Failure>,
        _ d: Publisher<D, Failure>
    ) -> Publisher<(Output, B, C, D), Failure> {
        zip(b).zip(c).zip(d).map { ($0.0.0.0, $0.0.0.1, $0.0.1, $0.1) }
    }

    // Transform-closure overloads.

    func zip<B: Sendable, T: Sendable>(
        _ other: Publisher<B, Failure>,
        _ transform: @escaping @Sendable (Output, B) -> T
    ) -> Publisher<T, Failure> {
        zip(other).map(transform)
    }

    func zip<B: Sendable, C: Sendable, T: Sendable>(
        _ b: Publisher<B, Failure>,
        _ c: Publisher<C, Failure>,
        _ transform: @escaping @Sendable (Output, B, C) -> T
    ) -> Publisher<T, Failure> {
        zip(b, c).map { transform($0.0, $0.1, $0.2) }
    }

    func zip<B: Sendable, C: Sendable, D: Sendable, T: Sendable>(
        _ b: Publisher<B, Failure>,
        _ c: Publisher<C, Failure>,
        _ d: Publisher<D, Failure>,
        _ transform: @escaping @Sendable (Output, B, C, D) -> T
    ) -> Publisher<T, Failure> {
        zip(b, c, d).map { transform($0.0, $0.1, $0.2, $0.3) }
    }
}

// MARK: - combineLatest

public extension Publisher {
    // swiftlint:disable:next cyclomatic_complexity
    func combineLatest<B: Sendable>(
        _ other: Publisher<B, Failure>
    ) -> Publisher<(Output, B), Failure> {
        let selfFactory = _stream.factory
        let otherFactory = other._stream.factory
        return Publisher<(Output, B), Failure>(DeferredStream {
            // StreamBox is required here: the FIFO task-group pattern shares one iterator
            // across successive @Sendable child closures — exactly one child calls next() at a time.
            let selfBox = StreamBox<Result<Output, Failure>>(selfFactory())
            let otherBox = StreamBox<Result<B, Failure>>(otherFactory())
            return AsyncStream<Result<(Output, B), Failure>> { raw in
                let task = Task {
                    var latestA: Output?
                    var latestB: B?

                    // Serialised interleaving: one pending task per publisher.
                    // Processing one value at a time in completion (arrival) order
                    // preserves temporal ordering even when values are pre-buffered.
                    typealias _Event = _CombineEvent<Result<Output, Failure>, Result<B, Failure>>
                    await withTaskGroup(of: _Event.self) { group in
                        group.addTask { .fromA(await selfBox.next()) }
                        group.addTask { .fromB(await otherBox.next()) }

                        var aOpen = true
                        var bOpen = true

                        while aOpen || bOpen, let event = await group.next() {
                            switch event {
                            case let .fromA(.some(result)):
                                switch result {
                                case let .success(a):
                                    latestA = a
                                    if let b = latestB {
                                        if case .terminated = raw.yield(Result.success((a, b))) { return }
                                    }
                                    group.addTask { .fromA(await selfBox.next()) }
                                case let .failure(e):
                                    _ = raw.yield(Result.failure(e)); raw.finish(); return
                                }
                            case let .fromB(.some(result)):
                                switch result {
                                case let .success(b):
                                    latestB = b
                                    if let a = latestA {
                                        if case .terminated = raw.yield(Result.success((a, b))) { return }
                                    }
                                    group.addTask { .fromB(await otherBox.next()) }
                                case let .failure(e):
                                    _ = raw.yield(Result.failure(e)); raw.finish(); return
                                }
                            // If a source closes without ever emitting, no tuple can ever
                            // be formed — terminate immediately rather than draining the other source.
                            case .fromA(.none):
                                if latestA == nil { raw.finish(); return }
                                aOpen = false
                            case .fromB(.none):
                                if latestB == nil { raw.finish(); return }
                                bOpen = false
                            }
                        }
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }

    func combineLatest<B: Sendable, C: Sendable>(
        _ b: Publisher<B, Failure>,
        _ c: Publisher<C, Failure>
    ) -> Publisher<(Output, B, C), Failure> {
        combineLatest(b).combineLatest(c).map { ($0.0.0, $0.0.1, $0.1) }
    }

    func combineLatest<B: Sendable, C: Sendable, D: Sendable>(
        _ b: Publisher<B, Failure>,
        _ c: Publisher<C, Failure>,
        _ d: Publisher<D, Failure>
    ) -> Publisher<(Output, B, C, D), Failure> {
        combineLatest(b).combineLatest(c).combineLatest(d).map { ($0.0.0.0, $0.0.0.1, $0.0.1, $0.1) }
    }

    // Transform-closure overloads (syntactic sugar over .map on the tuple result).

    func combineLatest<B: Sendable, T: Sendable>(
        _ other: Publisher<B, Failure>,
        _ transform: @escaping @Sendable (Output, B) -> T
    ) -> Publisher<T, Failure> {
        combineLatest(other).map(transform)
    }

    func combineLatest<B: Sendable, C: Sendable, T: Sendable>(
        _ b: Publisher<B, Failure>,
        _ c: Publisher<C, Failure>,
        _ transform: @escaping @Sendable (Output, B, C) -> T
    ) -> Publisher<T, Failure> {
        combineLatest(b, c).map { transform($0.0, $0.1, $0.2) }
    }

    func combineLatest<B: Sendable, C: Sendable, D: Sendable, T: Sendable>(
        _ b: Publisher<B, Failure>,
        _ c: Publisher<C, Failure>,
        _ d: Publisher<D, Failure>,
        _ transform: @escaping @Sendable (Output, B, C, D) -> T
    ) -> Publisher<T, Failure> {
        combineLatest(b, c, d).map { transform($0.0, $0.1, $0.2, $0.3) }
    }
}
