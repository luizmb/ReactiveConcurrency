// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// PublisherTValidation: outer = Publisher, inner = Validation
// Type: Publisher<Validation<E, A>, F>. Applicative accumulates errors (Validation's key property).
/// error Semigroup.

/// Applicative liftA2 for the Publisher-over-Validation stack: runs both effects and combines their results with fn; failures accumulate via the
public func liftA2TPublisherValidation<E: Semigroup & Sendable, A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (Publisher<Validation<E, A>, F>, Publisher<Validation<E, B>, F>) -> Publisher<Validation<E, C>, F> {
    { @Sendable pa, pb in
        pa.zip(pb).map { pair in Validation<E, C>.liftA2(fn)(pair.0, pair.1) }
    }
}

/// Applicative apply for the Publisher-over-Validation stack (zippy); failures accumulate via the error Semigroup.
public func applyTPublisherValidation<E: Semigroup & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fns: Publisher<Validation<E, @Sendable (A) -> B>, F>,
    _ values: Publisher<Validation<E, A>, F>
) -> Publisher<Validation<E, B>, F> {
    // Delegate the accumulation to Validation.apply (single source of truth) rather than
    // re-inlining the 4-case switch. NB: pairing is zippy (see Publisher.zip), so this inherits
    // zip semantics — the accumulation itself is correct (double failure combines via E.combine).
    fns.zip(values).map { pair in Validation<E, B>.apply(pair.0, pair.1) }
}
