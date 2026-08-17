package ai.soulside.transcript.dto;

import ai.soulside.meeting.model.SessionStatus;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * The complete, ordered transcript for a session, together with session metadata.
 */
public record TranscriptResponse(
        UUID meetingId,
        String meetingTitle,
        UUID sessionId,
        SessionStatus status,
        Instant startedAt,
        Instant endedAt,
        String transcriptUri,
        int segmentCount,
        List<TranscriptEntry> entries
) {
}
