// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// PublisherTValidation: outer = Publisher, inner = Validation
// Type: Publisher<Validation<E, A>, F>. Applicative accumulates errors (Validation's key property).
/// Applicative liftA2 for the Publisher-over-Validation stack: runs both effects and combines their results with fn; failures accumulate via the
/// error Semigroup.
func liftA2TPublisherValidation<E: Semigroup & Sendable, A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (Publisher<Validation<E, A>, F>, Publisher<Validation<E, B>, F>) -> Publisher<Validation<E, C>, F> {
    { @Sendable pa, pb in
        liftA2Publisher { (va: Validation<E, A>, vb: Validation<E, B>) in Validation<E, C>.liftA2(fn)(va, vb) }(pa, pb)
    }
}

/// Applicative apply for the Publisher-over-Validation stack (cartesian over the publisher, errors accumulated per pair); failures accumulate via the
/// error Semigroup.
func applyTPublisherValidation<E: Semigroup & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fns: Publisher<Validation<E, @Sendable (A) -> B>, F>,
    _ values: Publisher<Validation<E, A>, F>
) -> Publisher<Validation<E, B>, F> {
    // Delegate the accumulation to Validation.apply (single source of truth) rather than
    // re-inlining the 4-case switch. The publisher layer is the cartesian `ap`.
    liftA2Publisher { (vf: Validation<E, @Sendable (A) -> B>, va: Validation<E, A>) in Validation<E, B>.apply(vf, va) }(fns, values)
}
