import ReactiveConcurrency

let pipeline = [1, 2, 3, 4, 5].publisher
    .filter { $0.isMultiple(of: 2) }
    .map { $0 * 10 }

// `sink` is the boundary where execution happens. Keep the cancellable alive.
let cancellable = pipeline.sink { print($0) } // 20, then 40
