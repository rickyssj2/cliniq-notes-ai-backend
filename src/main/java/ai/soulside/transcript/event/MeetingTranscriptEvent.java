package ai.soulside.transcript.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.UUID;

/**
 * Deserialized {@code meeting.transcript} payload.
 *
 * <pre>
 * {
 *   "event": "meeting.transcript",
 *   "meeting": { "id": ..., "sessionId": ... },
 *   "data": {
 *     "transcriptId": ..., "sequenceNumber": 42,
 *     "speaker": { "id": ..., "name": ... },
 *     "content": ..., "startOffset": ..., "endOffset": ..., "language": "en"
 *   }
 * }
 * </pre>
 *
 * <p>Note: the meeting object is intentionally minimal — full meeting metadata arrives in
 * {@code meeting.started}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MeetingTranscriptEvent(String event, MeetingRef meeting, TranscriptData data) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record MeetingRef(UUID id, UUID sessionId) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TranscriptData(
            UUID transcriptId,
            Integer sequenceNumber,
            Speaker speaker,
            String content,
            String startOffset,
            String endOffset,
            String language
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Speaker(UUID id, String name) {
    }
}
