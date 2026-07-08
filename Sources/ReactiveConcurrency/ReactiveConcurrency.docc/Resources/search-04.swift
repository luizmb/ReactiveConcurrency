import ReactiveConcurrency

let cancellable = results.sink { hits in
    render(hits)
}

// Rapid typing: only the final term's search survives; the earlier ones are cancelled.
query.send("s")
query.send("sw")
query.send("swift")
