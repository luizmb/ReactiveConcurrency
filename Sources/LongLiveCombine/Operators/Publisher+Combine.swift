import Foundation
import CoreFP

private enum _CombineEvent<A: Sendable, B: Sendable>: Sendable {
    case fromA(A?)
    case fromB(B?)
}

// MARK: - merge

extension Publisher {
    public func merge(with other: Publisher<Output, Failure>) -> Publisher<Output, Failure> {
        let selfStream = _stream
        let otherStream = other._stream
        return Publisher<Output, Failure>(DeferredStream {
            // Pre-subscribe both streams synchronously so values sent after merge(...).sink()
            // are not lost while the inner Tasks are starting.
            let selfBox = _StreamBox<Result<Output, Failure>>(selfStream)
            let otherBox = _StreamBox<Result<Output, Failure>>(otherStream)
            return AsyncStream<Result<Output, Failure>> { raw in
                let task = Task {
                    await withTaskGroup(of: Void.self) { group in
                        group.addTask {
                            while let result = await selfBox.next() {
                                switch result {
                                case .success:
                                    if case .terminated = raw.yield(result) { return }
                                case .failure:
                                    _ = raw.yield(result); raw.finish(); return
                                }
                            }
                        }
                        group.addTask {
                            while let result = await otherBox.next() {
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

    public func merge(
        with b: Publisher<Output, Failure>,
        _ c: Publisher<Output, Failure>
    ) -> Publisher<Output, Failure> {
        merge(with: b).merge(with: c)
    }

    public func merge(
        with b: Publisher<Output, Failure>,
        _ c: Publisher<Output, Failure>,
        _ d: Publisher<Output, Failure>
    ) -> Publisher<Output, Failure> {
        merge(with: b).merge(with: c).merge(with: d)
    }
}

// MARK: - zip

extension Publisher {
    public func zip<B: Sendable>(
        _ other: Publisher<B, Failure>
    ) -> Publisher<(Output, B), Failure> {
        let selfStream = _stream
        let otherStream = other._stream
        return Publisher<(Output, B), Failure>(DeferredStream {
            let selfBox = _StreamBox<Result<Output, Failure>>(selfStream)
            let otherBox = _StreamBox<Result<B, Failure>>(otherStream)
            return AsyncStream<Result<(Output, B), Failure>> { raw in
                let task = Task {
                    while let sr = await selfBox.next() {
                        switch sr {
                        case .success(let a):
                            guard let or = await otherBox.next() else {
                                raw.finish(); return
                            }
                            switch or {
                            case .success(let b):
                                if case .terminated = raw.yield(Result.success((a, b))) { return }
                            case .failure(let e):
                                _ = raw.yield(Result.failure(e)); raw.finish(); return
                            }
                        case .failure(let e):
                            _ = raw.yield(Result.failure(e)); raw.finish(); return
                        }
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }

    public func zip<B: Sendable, C: Sendable>(
        _ b: Publisher<B, Failure>,
        _ c: Publisher<C, Failure>
    ) -> Publisher<(Output, B, C), Failure> {
        zip(b).zip(c).map { ($0.0.0, $0.0.1, $0.1) }
    }

    public func zip<B: Sendable, C: Sendable, D: Sendable>(
        _ b: Publisher<B, Failure>,
        _ c: Publisher<C, Failure>,
        _ d: Publisher<D, Failure>
    ) -> Publisher<(Output, B, C, D), Failure> {
        zip(b).zip(c).zip(d).map { ($0.0.0.0, $0.0.0.1, $0.0.1, $0.1) }
    }
}

// MARK: - combineLatest

extension Publisher {
    public func combineLatest<B: Sendable>(
        _ other: Publisher<B, Failure>
    ) -> Publisher<(Output, B), Failure> {
        let selfStream = _stream
        let otherStream = other._stream
        return Publisher<(Output, B), Failure>(DeferredStream {
            let selfBox = _StreamBox<Result<Output, Failure>>(selfStream)
            let otherBox = _StreamBox<Result<B, Failure>>(otherStream)
            return AsyncStream<Result<(Output, B), Failure>> { raw in
                let task = Task {
                    var latestA: Output? = nil
                    var latestB: B? = nil

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
                            case .fromA(.some(let result)):
                                switch result {
                                case .success(let a):
                                    latestA = a
                                    if let b = latestB {
                                        if case .terminated = raw.yield(Result.success((a, b))) { return }
                                    }
                                    group.addTask { .fromA(await selfBox.next()) }
                                case .failure(let e):
                                    _ = raw.yield(Result.failure(e)); raw.finish(); return
                                }
                            case .fromB(.some(let result)):
                                switch result {
                                case .success(let b):
                                    latestB = b
                                    if let a = latestA {
                                        if case .terminated = raw.yield(Result.success((a, b))) { return }
                                    }
                                    group.addTask { .fromB(await otherBox.next()) }
                                case .failure(let e):
                                    _ = raw.yield(Result.failure(e)); raw.finish(); return
                                }
                            case .fromA(.none): aOpen = false
                            case .fromB(.none): bOpen = false
                            }
                        }
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }

    public func combineLatest<B: Sendable, C: Sendable>(
        _ b: Publisher<B, Failure>,
        _ c: Publisher<C, Failure>
    ) -> Publisher<(Output, B, C), Failure> {
        combineLatest(b).combineLatest(c).map { ($0.0.0, $0.0.1, $0.1) }
    }

    public func combineLatest<B: Sendable, C: Sendable, D: Sendable>(
        _ b: Publisher<B, Failure>,
        _ c: Publisher<C, Failure>,
        _ d: Publisher<D, Failure>
    ) -> Publisher<(Output, B, C, D), Failure> {
        combineLatest(b).combineLatest(c).combineLatest(d).map { ($0.0.0.0, $0.0.0.1, $0.0.1, $0.1) }
    }
}

