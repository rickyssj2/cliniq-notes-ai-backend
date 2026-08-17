package ai.soulside.transcript;

import ai.soulside.meeting.UnknownSessionException;
import ai.soulside.meeting.model.Session;
import ai.soulside.meeting.repository.SessionRepository;
import ai.soulside.transcript.event.MeetingTranscriptEvent;
import ai.soulside.transcript.model.TranscriptSegment;
import ai.soulside.transcript.repository.TranscriptSegmentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Persists transcript segments belonging to a session.
 *
 * <p>Processing is idempotent: each segment is keyed by its {@code transcriptId} and duplicate
 * deliveries are skipped. If a transcript arrives before its session has been created (the
 * {@code meeting.started} event is still in flight), processing fails so the consumer can retry.
 */
@Service
public class TranscriptService {

    private static final Logger log = LoggerFactory.getLogger(TranscriptService.class);

    private final SessionRepository sessionRepository;
    private final TranscriptSegmentRepository segmentRepository;

    public TranscriptService(SessionRepository sessionRepository,
                             TranscriptSegmentRepository segmentRepository) {
        this.sessionRepository = sessionRepository;
        this.segmentRepository = segmentRepository;
    }

    @Transactional
    public void handleTranscript(MeetingTranscriptEvent event) {
        MeetingTranscriptEvent.TranscriptData data = event.data();
        UUID transcriptId = data.transcriptId();
        UUID sessionId = event.meeting().sessionId();

        // 1. Deduplicate on transcriptId (the natural idempotency key).
        if (segmentRepository.existsByTranscriptId(transcriptId)) {
            log.info("Duplicate transcript segment, skipping. transcriptId={} sessionId={}",
                    transcriptId, sessionId);
            return;
        }

        // 2. Ensure the session exists. If not, the started event may still be in flight —
        //    fail so the event is retried (and eventually dead-lettered if it never arrives).
        Session session = sessionRepository.findById(sessionId)
                .orElseThrow(() -> new UnknownSessionException(
                        "Transcript for unknown session: " + sessionId));

        // 3. Persist the segment. A concurrent duplicate is caught by the unique constraint.
        TranscriptSegment segment = TranscriptSegment.builder()
                .transcriptId(transcriptId)
                .session(session)
                .sequenceNumber(data.sequenceNumber())
                .speakerId(data.speaker() != null ? data.speaker().id() : null)
                .speakerName(data.speaker() != null ? data.speaker().name() : null)
                .content(data.content())
                .startOffset(data.startOffset())
                .endOffset(data.endOffset())
                .language(data.language() != null ? data.language() : "en")
                .build();

        try {
            segmentRepository.save(segment);
            log.info("Stored transcript segment. transcriptId={} sessionId={} seq={}",
                    transcriptId, sessionId, data.sequenceNumber());
        } catch (DataIntegrityViolationException e) {
            // Lost a race with a concurrent identical delivery — the row already exists.
            log.info("Concurrent duplicate transcript segment ignored. transcriptId={}", transcriptId);
        }
    }
}
