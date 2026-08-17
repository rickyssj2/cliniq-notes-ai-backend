package ai.soulside.transcript.dto;

import java.util.UUID;

/**
 * A single ordered entry in a session transcript.
 */
public record TranscriptEntry(
        int sequenceNumber,
        UUID speakerId,
        String speakerName,
        String content,
        Integer startOffsetSeconds,
        Integer endOffsetSeconds,
        String language
) {
}
