import CoreFP

// Wraps a DeferredStream iterator for safe capture in @Sendable Task closures.
// makeAsyncIterator() is called synchronously at init time — for subject-backed streams
// this registers the subscription before the Task starts, matching Combine's guarantee.
// @unchecked Sendable: safe because exactly one Task drives _iterator — no concurrent access.
final class _StreamBox<Element: Sendable>: @unchecked Sendable {
    private var _iterator: AsyncStream<Element>.AsyncIterator

    init(_ stream: DeferredStream<Element>) {
        _iterator = stream.makeAsyncIterator()
    }

    func next() async -> Element? {
        await _iterator.next()
    }
}
