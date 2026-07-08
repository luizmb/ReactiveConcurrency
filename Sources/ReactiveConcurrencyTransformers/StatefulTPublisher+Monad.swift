// SPDX-License-Identifier: Apache-2.0

// StatefulTPublisher: outer = Stateful, inner = Publisher
// Type: Stateful<S, Publisher<A, F>>
//
// flatMapT is not implementable for this stack (same limitation as StatefulTDeferredTask /
// StatefulTDeferredStream): Swift's `@Sendable` async closures cannot capture the `inout` state
// that Stateful threads, so the state mutation and the async Publisher execution cannot be safely
// composed. Stateful ships Functor + Applicative only for every effect inner.
// Use Reader<Env, Publisher<A, F>> (ReaderTPublisher) if you need monadic chaining with a
// read-only environment instead of mutable state.
