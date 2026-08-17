package ai.soulside.common;

/**
 * Constants for correlation-id propagation across HTTP, logging (MDC), and Kafka headers.
 */
public final class CorrelationId {

    private CorrelationId() {
    }

    /** HTTP header and Kafka header name carrying the correlation id. */
    public static final String HEADER = "X-Correlation-Id";

    /** MDC key used in log patterns. */
    public static final String MDC_KEY = "correlationId";
}
