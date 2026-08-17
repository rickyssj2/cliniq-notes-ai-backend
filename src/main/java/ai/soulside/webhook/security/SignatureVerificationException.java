package ai.soulside.webhook.security;

/**
 * Raised when an inbound webhook fails HMAC signature verification.
 * Mapped to HTTP 401 by the global exception handler.
 */
public class SignatureVerificationException extends RuntimeException {

    public SignatureVerificationException(String message) {
        super(message);
    }
}
