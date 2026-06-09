public enum Subscribers {}

extension Subscribers {
    public enum Completion<Failure: Error>: Sendable where Failure: Sendable {
        case finished
        case failure(Failure)
    }
}
