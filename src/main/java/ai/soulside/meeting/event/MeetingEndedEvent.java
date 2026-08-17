package ai.soulside.meeting.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

/**
 * Deserialized {@code meeting.ended} payload.
 *
 * <pre>
 * {
 *   "event": "meeting.ended",
 *   "meeting": { id, sessionId, title, status, createdAt, startedAt, endedAt, organizedBy },
 *   "reason": "HOST_ENDED_MEETING"
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MeetingEndedEvent(String event, MeetingPayload meeting, String reason) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record MeetingPayload(
            UUID id,
            UUID sessionId,
            String title,
            String status,
            Instant createdAt,
            Instant startedAt,
            Instant endedAt,
            Organizer organizedBy
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Organizer(UUID id, String name) {
    }
}
