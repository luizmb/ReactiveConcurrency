// Sequence.publisher — mirrors Combine's `[1, 2, 3].publisher`.

extension Sequence where Self: Sendable, Element: Sendable {
    /// A cold publisher that emits every element of the sequence, then finishes.
    public var publisher: Publisher<Element, Never> {
        Publisher<Element, Never>.sequence(self)
    }
}
