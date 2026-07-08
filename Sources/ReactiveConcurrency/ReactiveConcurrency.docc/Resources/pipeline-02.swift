import ReactiveConcurrency

// Compose operators. Still nothing runs — this is just a bigger value.
let pipeline = [1, 2, 3, 4, 5].publisher
    .filter { $0.isMultiple(of: 2) } // → 2, 4
    .map { $0 * 10 } // → 20, 40
