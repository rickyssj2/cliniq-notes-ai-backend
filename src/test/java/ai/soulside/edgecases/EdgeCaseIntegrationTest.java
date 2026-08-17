package ai.soulside.edgecases;

import ai.soulside.common.KafkaTopics;
import ai.soulside.meeting.model.SessionStatus;
import ai.soulside.meeting.repository.MeetingRepository;
import ai.soulside.meeting.repository.SessionRepository;
import ai.soulside.transcript.repository.TranscriptSegmentRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Scenarios beyond the happy path. Each documents an explicit, defensible behavior decision;
 * the matching rationale lives in DESIGN.md under "Edge Case Behavior".
 */
@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {KafkaTopics.MEETING_EVENTS, KafkaTopics.TRANSCRIPT_RECONSTRUCT})
class EdgeCaseIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private MeetingRepository meetingRepository;
    @Autowired
    private SessionRepository sessionRepository;
    @Autowired
    private TranscriptSegmentRepository segmentRepository;

    @BeforeEach
    void setUp() {
        segmentRepository.deleteAll();
        sessionRepository.deleteAll();
        meetingRepository.deleteAll();
    }

    private void publish(String sessionId, String eventType, String payload) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(KafkaTopics.MEETING_EVENTS, sessionId, payload);
        record.headers().add(new RecordHeader("eventType",
                eventType.getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(record);
    }

    private String started(UUID meetingId, UUID sessionId) {
        return """
                { "event": "meeting.started",
                  "meeting": { "id": "%s", "sessionId": "%s", "title": "Edge",
                    "startedAt": "2024-12-13T06:57:09.736Z", "createdAt": "2024-12-13T06:57:09.736Z" } }
                """.formatted(meetingId, sessionId);
    }

    private String ended(UUID meetingId, UUID sessionId) {
        return """
                { "event": "meeting.ended",
                  "meeting": { "id": "%s", "sessionId": "%s", "endedAt": "2024-12-13T07:04:37.052Z" },
                  "reason": "HOST_ENDED_MEETING" }
                """.formatted(meetingId, sessionId);
    }

    private String transcript(UUID meetingId, UUID sessionId, UUID transcriptId, int seq, String content) {
        return """
                { "event": "meeting.transcript",
                  "meeting": { "id": "%s", "sessionId": "%s" },
                  "data": { "transcriptId": "%s", "sequenceNumber": %d,
                    "speaker": { "id": "70c5d391-5bca-4cf3-9907-bec205798adb", "name": "Alice" },
                    "content": "%s", "startOffset": "%d", "endOffset": "%d", "language": "en" } }
                """.formatted(meetingId, sessionId, transcriptId, seq, content, seq, seq + 1);
    }

    /**
     * Decision: a transcript that arrives after {@code meeting.ended} is still persisted (no data
     * loss) and is visible via the read API, even though the session is already ENDED. It is not
     * automatically re-assembled into the stored file.
     */
    @Test
    void transcriptArrivingAfterEndedIsStillStored() {
        UUID meetingId = UUID.randomUUID();
        UUID sessionId = UUID.randomUUID();

        publish(sessionId.toString(), "meeting.started", started(meetingId, sessionId));
        publish(sessionId.toString(), "meeting.transcript",
                transcript(meetingId, sessionId, UUID.randomUUID(), 1, "during"));
        publish(sessionId.toString(), "meeting.ended", ended(meetingId, sessionId));

        // Wait for the session to be ENDED.
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(sessionRepository.findById(sessionId))
                        .get().satisfies(s -> assertThat(s.getStatus()).isEqualTo(SessionStatus.ENDED)));

        // A late transcript arrives after the session ended.
        UUID lateTranscriptId = UUID.randomUUID();
        publish(sessionId.toString(), "meeting.transcript",
                transcript(meetingId, sessionId, lateTranscriptId, 2, "late arrival"));

        // It is still persisted (no data loss); session stays ENDED.
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(segmentRepository.existsByTranscriptId(lateTranscriptId)).isTrue();
            assertThat(segmentRepository.findBySessionIdOrderBySequenceNumberAsc(sessionId))
                    .hasSize(2);
            assertThat(sessionRepository.findById(sessionId))
                    .get().satisfies(s -> assertThat(s.getStatus()).isEqualTo(SessionStatus.ENDED));
        });
    }

    /**
     * Decision: two concurrent sessions of the same meeting are tracked independently — each has
     * its own segments and lifecycle, sharing only the parent Meeting.
     */
    @Test
    void concurrentSessionsForSameMeetingTrackedIndependently() {
        UUID meetingId = UUID.randomUUID();
        UUID sessionA = UUID.randomUUID();
        UUID sessionB = UUID.randomUUID();

        publish(sessionA.toString(), "meeting.started", started(meetingId, sessionA));
        publish(sessionB.toString(), "meeting.started", started(meetingId, sessionB));

        publish(sessionA.toString(), "meeting.transcript",
                transcript(meetingId, sessionA, UUID.randomUUID(), 1, "A-one"));
        publish(sessionA.toString(), "meeting.transcript",
                transcript(meetingId, sessionA, UUID.randomUUID(), 2, "A-two"));
        publish(sessionB.toString(), "meeting.transcript",
                transcript(meetingId, sessionB, UUID.randomUUID(), 1, "B-one"));

        // End only session A.
        publish(sessionA.toString(), "meeting.ended", ended(meetingId, sessionA));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            // Both sessions exist under one meeting.
            assertThat(meetingRepository.findById(meetingId)).isPresent();
            assertThat(sessionRepository.findByMeetingIdAndStatus(meetingId, SessionStatus.ENDED))
                    .extracting(s -> s.getId()).containsExactly(sessionA);
            assertThat(sessionRepository.findByMeetingIdAndStatus(meetingId, SessionStatus.LIVE))
                    .extracting(s -> s.getId()).containsExactly(sessionB);

            // Segments are attributed to the correct session.
            assertThat(segmentRepository.findBySessionIdOrderBySequenceNumberAsc(sessionA))
                    .hasSize(2)
                    .extracting(s -> s.getContent()).containsExactly("A-one", "A-two");
            assertThat(segmentRepository.findBySessionIdOrderBySequenceNumberAsc(sessionB))
                    .hasSize(1)
                    .extracting(s -> s.getContent()).containsExactly("B-one");
        });
    }
}
