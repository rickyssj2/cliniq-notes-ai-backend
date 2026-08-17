package ai.soulside.meeting.repository;

import ai.soulside.meeting.model.Meeting;
import ai.soulside.meeting.model.Session;
import ai.soulside.meeting.model.SessionStatus;
import ai.soulside.transcript.model.TranscriptSegment;
import ai.soulside.transcript.repository.TranscriptSegmentRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {"meeting.events"})
class RepositoryIntegrationTest {

    @Autowired
    private MeetingRepository meetingRepository;

    @Autowired
    private SessionRepository sessionRepository;

    @Autowired
    private TranscriptSegmentRepository transcriptSegmentRepository;

    @Test
    void shouldPersistMeetingAndSession() {
        UUID meetingId = UUID.randomUUID();
        Meeting meeting = Meeting.builder()
                .id(meetingId)
                .title("Q4 Planning")
                .roomName("room-abc")
                .organizedById(UUID.randomUUID())
                .organizedByName("Alice")
                .createdAt(Instant.now())
                .build();
        meetingRepository.save(meeting);

        UUID sessionId = UUID.randomUUID();
        Session session = Session.builder()
                .id(sessionId)
                .meeting(meeting)
                .status(SessionStatus.LIVE)
                .startedAt(Instant.now())
                .build();
        sessionRepository.save(session);

        // Verify retrieval
        assertThat(sessionRepository.findByMeetingIdAndStatus(meetingId, SessionStatus.LIVE))
                .hasSize(1)
                .first()
                .satisfies(s -> {
                    assertThat(s.getId()).isEqualTo(sessionId);
                    assertThat(s.getStatus()).isEqualTo(SessionStatus.LIVE);
                });
    }

    @Test
    void shouldPersistTranscriptSegmentsInOrder() {
        Meeting meeting = meetingRepository.save(Meeting.builder()
                .id(UUID.randomUUID())
                .title("Standup")
                .createdAt(Instant.now())
                .build());

        Session session = sessionRepository.save(Session.builder()
                .id(UUID.randomUUID())
                .meeting(meeting)
                .status(SessionStatus.LIVE)
                .startedAt(Instant.now())
                .build());

        // Insert segments out of order
        transcriptSegmentRepository.save(TranscriptSegment.builder()
                .transcriptId(UUID.randomUUID())
                .session(session)
                .sequenceNumber(3)
                .speakerName("Bob")
                .content("Third chunk")
                .startOffset("00:00:10")
                .endOffset("00:00:15")
                .build());

        transcriptSegmentRepository.save(TranscriptSegment.builder()
                .transcriptId(UUID.randomUUID())
                .session(session)
                .sequenceNumber(1)
                .speakerName("Alice")
                .content("First chunk")
                .startOffset("00:00:00")
                .endOffset("00:00:05")
                .build());

        transcriptSegmentRepository.save(TranscriptSegment.builder()
                .transcriptId(UUID.randomUUID())
                .session(session)
                .sequenceNumber(2)
                .speakerName("Alice")
                .content("Second chunk")
                .startOffset("00:00:05")
                .endOffset("00:00:10")
                .build());

        // Query should return ordered by sequence_number
        List<TranscriptSegment> segments =
                transcriptSegmentRepository.findBySessionIdOrderBySequenceNumberAsc(session.getId());

        assertThat(segments).hasSize(3);
        assertThat(segments.get(0).getSequenceNumber()).isEqualTo(1);
        assertThat(segments.get(1).getSequenceNumber()).isEqualTo(2);
        assertThat(segments.get(2).getSequenceNumber()).isEqualTo(3);
        assertThat(segments.get(0).getContent()).isEqualTo("First chunk");
    }

    @Test
    void shouldRejectDuplicateTranscriptId() {
        Meeting meeting = meetingRepository.save(Meeting.builder()
                .id(UUID.randomUUID())
                .title("Dedup Test")
                .createdAt(Instant.now())
                .build());

        Session session = sessionRepository.save(Session.builder()
                .id(UUID.randomUUID())
                .meeting(meeting)
                .status(SessionStatus.LIVE)
                .startedAt(Instant.now())
                .build());

        UUID transcriptId = UUID.randomUUID();

        transcriptSegmentRepository.save(TranscriptSegment.builder()
                .transcriptId(transcriptId)
                .session(session)
                .sequenceNumber(1)
                .speakerName("Alice")
                .content("Hello")
                .startOffset("0")
                .endOffset("3")
                .build());

        // Duplicate transcript_id should throw
        TranscriptSegment duplicate = TranscriptSegment.builder()
                .transcriptId(transcriptId)
                .session(session)
                .sequenceNumber(1)
                .speakerName("Alice")
                .content("Hello again")
                .startOffset("0")
                .endOffset("3")
                .build();

        assertThatThrownBy(() -> {
            transcriptSegmentRepository.saveAndFlush(duplicate);
        }).isInstanceOf(Exception.class);
    }

    @Test
    void shouldCheckExistsByTranscriptId() {
        Meeting meeting = meetingRepository.save(Meeting.builder()
                .id(UUID.randomUUID())
                .title("Exists Test")
                .createdAt(Instant.now())
                .build());

        Session session = sessionRepository.save(Session.builder()
                .id(UUID.randomUUID())
                .meeting(meeting)
                .status(SessionStatus.LIVE)
                .startedAt(Instant.now())
                .build());

        UUID transcriptId = UUID.randomUUID();

        assertThat(transcriptSegmentRepository.existsByTranscriptId(transcriptId)).isFalse();

        transcriptSegmentRepository.save(TranscriptSegment.builder()
                .transcriptId(transcriptId)
                .session(session)
                .sequenceNumber(1)
                .speakerName("Bob")
                .content("Check exists")
                .startOffset("0")
                .endOffset("2")
                .build());

        assertThat(transcriptSegmentRepository.existsByTranscriptId(transcriptId)).isTrue();
    }
}
