// SPDX-License-Identifier: Apache-2.0

private enum _CombineEvent<A: Sendable, B: Sendable>: Sendable {
    case fromA(A?)
    case fromB(B?)
}

// MARK: - merge

public extension Publisher {
    /// Interleaves the elements of this publisher and another, emitting them in arrival order.
    /// - Returns: A publisher that emits from both sources; the first failure seals the stream.
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

    /// Interleaves the elements of this publisher and two others, emitting them in arrival order.
    /// - Returns: A publisher that emits from all three sources; the first failure seals the stream.
    func merge(
        with b: Publisher<Output, Failure>,
        _ c: Publisher<Output, Failure>
    ) -> Publisher<Output, Failure> {
        merge(with: b).merge(with: c)
    }

    /// Interleaves the elements of this publisher and three others, emitting them in arrival order.
    /// - Returns: A publisher that emits from all four sources; the first failure seals the stream.
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
    // swiftlint:disable cyclomatic_complexity
    /// Pairs each element of this publisher positionally with the corresponding element of another into a typed tuple.
    ///
    /// Zippy and index-aligned: the *n*-th output pairs each side's *n*-th element; completion occurs as soon as
    /// either side can no longer form a pair.
    /// - Returns: A publisher of `(Output, B)` tuples.
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

    // swiftlint:enable cyclomatic_complexity
    /// Positionally pairs elements of this publisher with two others into a typed `(Output, B, C)` tuple.
    /// - Returns: A publisher of three-element tuples, index-aligned across all sources.
    func zip<B: Sendable, C: Sendable>(
        _ b: Publisher<B, Failure>,
        _ c: Publisher<C, Failure>
    ) -> Publisher<(Output, B, C), Failure> {
        zip(b).zip(c).map { ($0.0.0, $0.0.1, $0.1) }
    }

    /// Positionally pairs elements of this publisher with three others into a typed `(Output, B, C, D)` tuple.
    /// - Returns: A publisher of four-element tuples, index-aligned across all sources.
    func zip<B: Sendable, C: Sendable, D: Sendable>(
        _ b: Publisher<B, Failure>,
        _ c: Publisher<C, Failure>,
        _ d: Publisher<D, Failure>
    ) -> Publisher<(Output, B, C, D), Failure> {
        zip(b).zip(c).zip(d).map { ($0.0.0.0, $0.0.0.1, $0.0.1, $0.1) }
    }

    // Transform-closure overloads.

    /// Zips this publisher with another, combining each positional pair through `transform`.
    /// - Parameters:
    ///   - other: The publisher to zip with.
    ///   - transform: A closure combining each aligned pair into a single value.
    func zip<B: Sendable, T: Sendable>(
        _ other: Publisher<B, Failure>,
        _ transform: @escaping @Sendable (Output, B) -> T
    ) -> Publisher<T, Failure> {
        zip(other).map(transform)
    }

    /// Zips this publisher with two others, combining each positional triple through `transform`.
    func zip<B: Sendable, C: Sendable, T: Sendable>(
        _ b: Publisher<B, Failure>,
        _ c: Publisher<C, Failure>,
        _ transform: @escaping @Sendable (Output, B, C) -> T
    ) -> Publisher<T, Failure> {
        zip(b, c).map { transform($0.0, $0.1, $0.2) }
    }

    /// Zips this publisher with three others, combining each positional quadruple through `transform`.
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
    // swiftlint:disable cyclomatic_complexity
    /// Emits a tuple of the most recent element from each publisher whenever either one emits.
    ///
    /// Requires both sources to have emitted at least once before the first tuple is produced.
    /// - Returns: A publisher of `(Output, B)` tuples of the latest values.
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

    // swiftlint:enable cyclomatic_complexity
    /// Emits a tuple of the most recent element from this publisher and two others whenever any of them emits.
    /// - Returns: A publisher of `(Output, B, C)` tuples of the latest values.
    func combineLatest<B: Sendable, C: Sendable>(
        _ b: Publisher<B, Failure>,
        _ c: Publisher<C, Failure>
    ) -> Publisher<(Output, B, C), Failure> {
        combineLatest(b).combineLatest(c).map { ($0.0.0, $0.0.1, $0.1) }
    }

    /// Emits a tuple of the most recent element from this publisher and three others whenever any of them emits.
    /// - Returns: A publisher of `(Output, B, C, D)` tuples of the latest values.
    func combineLatest<B: Sendable, C: Sendable, D: Sendable>(
        _ b: Publisher<B, Failure>,
        _ c: Publisher<C, Failure>,
        _ d: Publisher<D, Failure>
    ) -> Publisher<(Output, B, C, D), Failure> {
        combineLatest(b).combineLatest(c).combineLatest(d).map { ($0.0.0.0, $0.0.0.1, $0.0.1, $0.1) }
    }

    // Transform-closure overloads (syntactic sugar over .map on the tuple result).

    /// Combines the latest element of this publisher and another through `transform` whenever either emits.
    func combineLatest<B: Sendable, T: Sendable>(
        _ other: Publisher<B, Failure>,
        _ transform: @escaping @Sendable (Output, B) -> T
    ) -> Publisher<T, Failure> {
        combineLatest(other).map(transform)
    }

    /// Combines the latest element of this publisher and two others through `transform` whenever any emits.
    func combineLatest<B: Sendable, C: Sendable, T: Sendable>(
        _ b: Publisher<B, Failure>,
        _ c: Publisher<C, Failure>,
        _ transform: @escaping @Sendable (Output, B, C) -> T
    ) -> Publisher<T, Failure> {
        combineLatest(b, c).map { transform($0.0, $0.1, $0.2) }
    }

    /// Combines the latest element of this publisher and three others through `transform` whenever any emits.
    func combineLatest<B: Sendable, C: Sendable, D: Sendable, T: Sendable>(
        _ b: Publisher<B, Failure>,
        _ c: Publisher<C, Failure>,
        _ d: Publisher<D, Failure>,
        _ transform: @escaping @Sendable (Output, B, C, D) -> T
    ) -> Publisher<T, Failure> {
        combineLatest(b, c, d).map { transform($0.0, $0.1, $0.2, $0.3) }
    }
}
