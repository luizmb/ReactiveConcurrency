import ReactiveConcurrency

// Recover with a fallback publisher, inspecting the typed error.
let recovered = load
    .catch { error in
        switch error {
        case .offline: cachedData.publisher.setFailureType(to: LoadError.self)
        case .notFound: Publisher<Data, LoadError>.just(Data())
        }
    }
