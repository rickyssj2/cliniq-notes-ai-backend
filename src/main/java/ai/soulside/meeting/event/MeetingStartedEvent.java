package ai.soulside.meeting.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

/**
 * Deserialized {@code meeting.started} payload.
 *
 * <pre>
 * {
 *   "event": "meeting.started",
 *   "meeting": { id, sessionId, title, roomName, status, createdAt, startedAt, organizedBy }
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MeetingStartedEvent(String event, MeetingPayload meeting) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record MeetingPayload(
            UUID id,
            UUID sessionId,
            String title,
            String roomName,
            String status,
            Instant createdAt,
            Instant startedAt,
            Organizer organizedBy
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Organizer(UUID id, String name) {
    }
}
