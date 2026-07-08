import ReactiveConcurrency

// share() multicasts one upstream run to every subscriber. It ref-counts:
// the upstream starts on the first subscriber and tears down when the last cancels.
let shared = expensive.share()

let a = shared.sink { use($0) }
let b = shared.sink { use($0) } // no second expensive run
