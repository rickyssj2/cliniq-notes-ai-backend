package ai.soulside.common;

/**
 * Central registry of Kafka topic names used across the service.
 */
public final class KafkaTopics {

    private KafkaTopics() {
    }

    /** Primary ingestion topic — all inbound webhook events land here, keyed by sessionId. */
    public static final String MEETING_EVENTS = "meeting.events";

    /** Task topic — signals that a session's transcript should be reconstructed. */
    public static final String TRANSCRIPT_RECONSTRUCT = "transcript.reconstruct";

    /** Dead-letter topic for events that exhaust retries. */
    public static final String MEETING_EVENTS_DLT = "meeting.events.DLT";

    /** Dead-letter topic for reconstruction tasks that exhaust retries. */
    public static final String TRANSCRIPT_RECONSTRUCT_DLT = "transcript.reconstruct.DLT";
}
