package ai.soulside.transcript;

import ai.soulside.common.web.ResourceNotFoundException;
import ai.soulside.meeting.model.Session;
import ai.soulside.meeting.repository.SessionRepository;
import ai.soulside.transcript.dto.TranscriptEntry;
import ai.soulside.transcript.dto.TranscriptResponse;
import ai.soulside.transcript.model.TranscriptSegment;
import ai.soulside.transcript.repository.TranscriptSegmentRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Read-side service that assembles the complete, ordered transcript for a session.
 *
 * <p>Segments are always ordered from the database by {@code sequenceNumber}, so the response is
 * correct for both LIVE sessions (still accumulating segments) and ENDED sessions (already
 * reconstructed to storage). The stored file URI is included for consumers that want the
 * assembled artifact, but the structured entries are sourced directly from the database.
 */
@Service
@Transactional(readOnly = true)
public class TranscriptQueryService {

    private final SessionRepository sessionRepository;
    private final TranscriptSegmentRepository segmentRepository;

    public TranscriptQueryService(SessionRepository sessionRepository,
                                  TranscriptSegmentRepository segmentRepository) {
        this.sessionRepository = sessionRepository;
        this.segmentRepository = segmentRepository;
    }

    public TranscriptResponse getTranscript(UUID meetingId, UUID sessionId) {
        Session session = sessionRepository.findByIdAndMeetingId(sessionId, meetingId)
                .orElseThrow(() -> new ResourceNotFoundException(
                        "No session " + sessionId + " for meeting " + meetingId));

        List<TranscriptSegment> segments =
                segmentRepository.findBySessionIdOrderBySequenceNumberAsc(sessionId);

        List<TranscriptEntry> entries = segments.stream()
                .map(this::toEntry)
                .toList();

        return new TranscriptResponse(
                meetingId,
                session.getMeeting().getTitle(),
                sessionId,
                session.getStatus(),
                session.getStartedAt(),
                session.getEndedAt(),
                session.getTranscriptUri(),
                entries.size(),
                entries);
    }

    private TranscriptEntry toEntry(TranscriptSegment s) {
        return new TranscriptEntry(
                s.getSequenceNumber(),
                s.getSpeakerId(),
                s.getSpeakerName(),
                s.getContent(),
                s.getStartOffset(),
                s.getEndOffset(),
                s.getLanguage());
    }
}
