package ai.soulside.common.web;

/**
 * Raised when a requested resource (meeting, session, transcript) does not exist.
 * Mapped to HTTP 404 by the global exception handler.
 */
public class ResourceNotFoundException extends RuntimeException {

    public ResourceNotFoundException(String message) {
        super(message);
    }
}
