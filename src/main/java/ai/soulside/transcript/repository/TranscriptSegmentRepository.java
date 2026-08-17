package ai.soulside.transcript.repository;

import ai.soulside.transcript.model.TranscriptSegment;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface TranscriptSegmentRepository extends JpaRepository<TranscriptSegment, Long> {

    /**
     * Retrieve all transcript segments for a session, ordered by sequence number.
     * This is the primary query for reconstructing a full transcript.
     */
    List<TranscriptSegment> findBySessionIdOrderBySequenceNumberAsc(UUID sessionId);

    /**
     * Check if a segment with the given transcriptId already exists (deduplication).
     */
    boolean existsByTranscriptId(UUID transcriptId);
}
