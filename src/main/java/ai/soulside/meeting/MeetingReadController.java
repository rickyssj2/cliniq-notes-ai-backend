package ai.soulside.meeting;

import ai.soulside.transcript.TranscriptQueryService;
import ai.soulside.transcript.dto.TranscriptResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * Read API for meeting/session data.
 */
@RestController
@RequestMapping("/api/v1/meetings")
public class MeetingReadController {

    private final TranscriptQueryService transcriptQueryService;

    public MeetingReadController(TranscriptQueryService transcriptQueryService) {
        this.transcriptQueryService = transcriptQueryService;
    }

    /**
     * Return the complete, ordered transcript for a session.
     *
     * @return 200 with the transcript, or 404 if the meeting/session pair is unknown
     */
    @GetMapping("/{meetingId}/sessions/{sessionId}/transcript")
    public TranscriptResponse getTranscript(@PathVariable UUID meetingId,
                                            @PathVariable UUID sessionId) {
        return transcriptQueryService.getTranscript(meetingId, sessionId);
    }
}
