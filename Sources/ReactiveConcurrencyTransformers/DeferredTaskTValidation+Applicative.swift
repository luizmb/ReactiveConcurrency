// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// DeferredTaskTValidation: outer = DeferredTask, inner = Validation
// Type: DeferredTask<Validation<E, A>>
// Applicative accumulates errors (Validation's key property)

public func liftA2TDeferredTaskValidation<E: Semigroup & Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (DeferredTask<Validation<E, A>>, DeferredTask<Validation<E, B>>) -> DeferredTask<Validation<E, C>> {
    { @Sendable ta, tb in
        DeferredTask<Validation<E, C>> {
            let va = await ta.run()
            let vb = await tb.run()
            return Validation<E, C>.liftA2(fn)(va, vb)
        }
    }
}

public func applyTDeferredTaskValidation<E: Semigroup & Sendable, A: Sendable, B: Sendable>(
    _ fns: DeferredTask<Validation<E, @Sendable (A) -> B>>,
    _ values: DeferredTask<Validation<E, A>>
) -> DeferredTask<Validation<E, B>> where B: Sendable {
    // Delegate the accumulation to Validation.apply — the single source of truth for the
    // 4-case switch (double failure combines via E.combine) — rather than re-inlining it.
    DeferredTask<Validation<E, B>> {
        Validation<E, B>.apply(await fns.run(), await values.run())
    }
}
