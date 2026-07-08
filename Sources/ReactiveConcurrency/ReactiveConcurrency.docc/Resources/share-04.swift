import ReactiveConcurrency

// For manual control, multicast through a subject and connect explicitly.
let connectable = expensive.multicast(subject: PassthroughSubject<Int, Never>())

let a = connectable.sink { use($0) }
let b = connectable.sink { use($0) }
let connection = connectable.connect() // start now; cancel `connection` to stop
