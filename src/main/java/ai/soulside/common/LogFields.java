package ai.soulside.common;

/**
 * MDC keys included in structured/plain logs for traceability.
 */
public final class LogFields {

    private LogFields() {
    }

    /** Session the log line pertains to. */
    public static final String SESSION_ID = "sessionId";

    /** Webhook event type the log line pertains to. */
    public static final String EVENT = "event";
}
