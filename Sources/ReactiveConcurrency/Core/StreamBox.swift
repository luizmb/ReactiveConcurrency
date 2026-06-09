// Boxes an AsyncStream iterator for use across multiple @Sendable task-group closures.
// Only used by the combineLatest FIFO pattern where one shared iterator is advanced by
// successive task-group children — exactly one child calls next() at a time, never concurrently.
// @unchecked Sendable: safe because the FIFO task-group pattern serialises all next() calls.
final class _StreamBox<Element: Sendable>: @unchecked Sendable {
    private var _iterator: AsyncStream<Element>.AsyncIterator

    init(_ stream: AsyncStream<Element>) {
        _iterator = stream.makeAsyncIterator()
    }

    func next() async -> Element? {
        await _iterator.next()
    }
}
