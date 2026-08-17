package ai.soulside.meeting;

/**
 * Raised when an event references a session that has never been started.
 * Propagated out of the consumer so the error handler can retry and eventually route to the DLQ.
 */
public class UnknownSessionException extends RuntimeException {

    public UnknownSessionException(String message) {
        super(message);
    }
}
