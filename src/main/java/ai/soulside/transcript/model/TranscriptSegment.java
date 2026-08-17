package ai.soulside.transcript.model;

import ai.soulside.meeting.model.Session;
import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/**
 * An individual chunk of transcribed speech belonging to a session.
 * Multiple segments arrive over the lifetime of a session and must be assembled in order.
 */
@Entity
@Table(name = "transcript_segments")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TranscriptSegment {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "transcript_id", nullable = false, unique = true)
    private UUID transcriptId;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "session_id", nullable = false)
    private Session session;

    @Column(name = "sequence_number", nullable = false)
    private Integer sequenceNumber;

    @Column(name = "speaker_id")
    private UUID speakerId;

    @Column(name = "speaker_name")
    private String speakerName;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String content;

    /** Offset from session start, in whole seconds. */
    @Column(name = "start_offset", nullable = false)
    private Integer startOffset;

    /** Offset from session start, in whole seconds. */
    @Column(name = "end_offset", nullable = false)
    private Integer endOffset;

    @Column(length = 10)
    @Builder.Default
    private String language = "en";

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @PrePersist
    protected void onCreate() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }
}
