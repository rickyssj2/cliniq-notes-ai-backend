package ai.soulside.webhook;

/**
 * Raised when a webhook payload is malformed or fails envelope validation.
 * Mapped to HTTP 400 by the global exception handler.
 */
public class InvalidWebhookPayloadException extends RuntimeException {

    public InvalidWebhookPayloadException(String message) {
        super(message);
    }
}
