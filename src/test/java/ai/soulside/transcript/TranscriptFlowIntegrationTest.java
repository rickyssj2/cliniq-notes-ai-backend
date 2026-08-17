package ai.soulside.transcript;

import ai.soulside.common.KafkaTopics;
import ai.soulside.meeting.repository.MeetingRepository;
import ai.soulside.meeting.repository.SessionRepository;
import ai.soulside.transcript.model.TranscriptSegment;
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
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {KafkaTopics.MEETING_EVENTS, KafkaTopics.TRANSCRIPT_RECONSTRUCT})
class TranscriptFlowIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private MeetingRepository meetingRepository;
    @Autowired
    private SessionRepository sessionRepository;
    @Autowired
    private TranscriptSegmentRepository segmentRepository;

    private UUID meetingId;
    private UUID sessionId;

    @BeforeEach
    void setUp() {
        segmentRepository.deleteAll();
        sessionRepository.deleteAll();
        meetingRepository.deleteAll();
        meetingId = UUID.randomUUID();
        sessionId = UUID.randomUUID();
    }

    private void publish(String eventType, String payload) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(KafkaTopics.MEETING_EVENTS, sessionId.toString(), payload);
        record.headers().add(new RecordHeader("eventType",
                eventType.getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(record);
    }

    private void publishStarted() {
        publish("meeting.started", """
                { "event": "meeting.started",
                  "meeting": { "id": "%s", "sessionId": "%s", "title": "T",
                    "startedAt": "2024-12-13T06:57:09.736Z", "createdAt": "2024-12-13T06:57:09.736Z" } }
                """.formatted(meetingId, sessionId));
    }

    private String transcriptPayload(UUID transcriptId, int seq, String content) {
        return """
                { "event": "meeting.transcript",
                  "meeting": { "id": "%s", "sessionId": "%s" },
                  "data": {
                    "transcriptId": "%s", "sequenceNumber": %d,
                    "speaker": { "id": "70c5d391-5bca-4cf3-9907-bec205798adb", "name": "Alice" },
                    "content": "%s", "startOffset": "00:00:0%d", "endOffset": "00:00:0%d", "language": "en"
                  } }
                """.formatted(meetingId, sessionId, transcriptId, seq, content, seq, seq + 1);
    }

    @Test
    void startedThenTranscriptChunksStoredInOrder() {
        publishStarted();
        UUID t1 = UUID.randomUUID();
        UUID t2 = UUID.randomUUID();
        UUID t3 = UUID.randomUUID();

        // Publish out of order (3, 1, 2) — storage preserves sequenceNumber ordering on read.
        publish("meeting.transcript", transcriptPayload(t3, 3, "third"));
        publish("meeting.transcript", transcriptPayload(t1, 1, "first"));
        publish("meeting.transcript", transcriptPayload(t2, 2, "second"));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            List<TranscriptSegment> segments =
                    segmentRepository.findBySessionIdOrderBySequenceNumberAsc(sessionId);
            assertThat(segments).hasSize(3);
            assertThat(segments).extracting(TranscriptSegment::getSequenceNumber)
                    .containsExactly(1, 2, 3);
            assertThat(segments).extracting(TranscriptSegment::getContent)
                    .containsExactly("first", "second", "third");
        });
    }

    @Test
    void duplicateTranscriptIdStoredOnce() {
        publishStarted();
        UUID transcriptId = UUID.randomUUID();

        publish("meeting.transcript", transcriptPayload(transcriptId, 1, "hello"));
        publish("meeting.transcript", transcriptPayload(transcriptId, 1, "hello"));

        await().atMost(Duration.ofSeconds(15))
                .until(() -> segmentRepository.existsByTranscriptId(transcriptId));

        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(4)).untilAsserted(() ->
                assertThat(segmentRepository
                        .findBySessionIdOrderBySequenceNumberAsc(sessionId)).hasSize(1));
    }

    @Test
    void segmentStoresAllFieldsCorrectly() {
        publishStarted();
        UUID transcriptId = UUID.randomUUID();
        publish("meeting.transcript", transcriptPayload(transcriptId, 1, "detailed content"));

        await().atMost(Duration.ofSeconds(15))
                .until(() -> segmentRepository.existsByTranscriptId(transcriptId));

        List<TranscriptSegment> segments =
                segmentRepository.findBySessionIdOrderBySequenceNumberAsc(sessionId);
        assertThat(segments).hasSize(1);
        TranscriptSegment s = segments.get(0);
        assertThat(s.getContent()).isEqualTo("detailed content");
        assertThat(s.getSpeakerName()).isEqualTo("Alice");
        assertThat(s.getSpeakerId())
                .isEqualTo(UUID.fromString("70c5d391-5bca-4cf3-9907-bec205798adb"));
        assertThat(s.getLanguage()).isEqualTo("en");
        assertThat(s.getStartOffset()).isEqualTo("00:00:01");
    }
}
