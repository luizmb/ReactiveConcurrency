import ReactiveConcurrency

// Two subscribers → the expensive work runs TWICE.
let a = expensive.sink { use($0) }
let b = expensive.sink { use($0) }
