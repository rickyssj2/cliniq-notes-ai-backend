package ai.soulside.transcript;

import ai.soulside.meeting.UnknownSessionException;
import ai.soulside.meeting.model.Session;
import ai.soulside.meeting.repository.SessionRepository;
import ai.soulside.storage.StorageService;
import ai.soulside.common.Metrics;
import ai.soulside.transcript.model.TranscriptSegment;
import ai.soulside.transcript.repository.TranscriptSegmentRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Assembles a session's transcript segments (in sequence order) into a single text document,
 * persists it via {@link StorageService}, and records the resulting URI on the session.
 */
@Service
public class TranscriptReconstructionService {

    private static final Logger log = LoggerFactory.getLogger(TranscriptReconstructionService.class);

    private final SessionRepository sessionRepository;
    private final TranscriptSegmentRepository segmentRepository;
    private final StorageService storageService;
    private final MeterRegistry meterRegistry;

    public TranscriptReconstructionService(SessionRepository sessionRepository,
                                           TranscriptSegmentRepository segmentRepository,
                                           StorageService storageService,
                                           MeterRegistry meterRegistry) {
        this.sessionRepository = sessionRepository;
        this.segmentRepository = segmentRepository;
        this.storageService = storageService;
        this.meterRegistry = meterRegistry;
    }

    @Transactional
    public void reconstruct(UUID sessionId) {
        Session session = sessionRepository.findById(sessionId)
                .orElseThrow(() -> new UnknownSessionException(
                        "Cannot reconstruct unknown session: " + sessionId));

        List<TranscriptSegment> segments =
                segmentRepository.findBySessionIdOrderBySequenceNumberAsc(sessionId);

        if (segments.isEmpty()) {
            log.warn("No transcript segments to reconstruct for sessionId={}", sessionId);
        }

        String content = format(segments);
        String uri = storageService.store(sessionId.toString(), content);

        session.setTranscriptUri(uri);
        sessionRepository.save(session);

        meterRegistry.counter(Metrics.TRANSCRIPT_RECONSTRUCTION_COUNT).increment();

        log.info("Reconstructed transcript. sessionId={} segments={} uri={}",
                sessionId, segments.size(), uri);
    }

    /**
     * Render segments as newline-separated lines: {@code [start-end] Speaker: content}.
     */
    String format(List<TranscriptSegment> segments) {
        StringBuilder sb = new StringBuilder();
        for (TranscriptSegment s : segments) {
            sb.append('[')
                    .append(s.getStartOffset()).append('-').append(s.getEndOffset())
                    .append("s] ")
                    .append(s.getSpeakerName() != null ? s.getSpeakerName() : "Unknown")
                    .append(": ")
                    .append(s.getContent())
                    .append('\n');
        }
        return sb.toString();
    }
}
