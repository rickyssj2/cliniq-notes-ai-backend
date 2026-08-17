package ai.soulside.common;

/**
 * Central registry of custom Micrometer metric names and tag keys.
 */
public final class Metrics {

    private Metrics() {
    }

    // ─── Counters ────────────────────────────────────────────────────────────
    /** Webhook events accepted at the HTTP edge, tagged by {@link #TAG_EVENT}. */
    public static final String WEBHOOK_EVENTS_RECEIVED = "webhook.events.received";

    /** Events consumed from Kafka, tagged by {@link #TAG_EVENT} and {@link #TAG_OUTCOME}. */
    public static final String CONSUMER_EVENTS_PROCESSED = "consumer.events.processed";

    /** Transcripts assembled by the reconstruction consumer. */
    public static final String TRANSCRIPT_RECONSTRUCTION_COUNT = "transcript.reconstruction.count";

    /** Records dead-lettered after exhausting retries or on non-retryable failures. */
    public static final String CONSUMER_DLQ_COUNT = "kafka.consumer.dlq.count";

    // ─── Timers ──────────────────────────────────────────────────────────────
    /** End-to-end processing time for a consumed event, tagged by {@link #TAG_EVENT}. */
    public static final String CONSUMER_PROCESSING_TIME = "consumer.event.processing.time";

    // ─── Tag keys ──────────────────────────────────────────────────────────────
    public static final String TAG_EVENT = "event";
    public static final String TAG_OUTCOME = "outcome";

    // ─── Tag values ──────────────────────────────────────────────────────────
    public static final String OUTCOME_SUCCESS = "success";
    public static final String OUTCOME_FAILURE = "failure";
}
