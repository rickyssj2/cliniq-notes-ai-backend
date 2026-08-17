package ai.soulside.transcript;

import ai.soulside.common.KafkaTopics;
import ai.soulside.meeting.repository.MeetingRepository;
import ai.soulside.meeting.repository.SessionRepository;
import ai.soulside.transcript.repository.TranscriptSegmentRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end: publishing the full meeting lifecycle drives the reconstruct consumer to write an
 * ordered transcript file and record its URI on the session.
 */
@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {KafkaTopics.MEETING_EVENTS, KafkaTopics.TRANSCRIPT_RECONSTRUCT})
class ReconstructionIntegrationTest {

    @TempDir
    static Path storageDir;

    @DynamicPropertySource
    static void storageProps(DynamicPropertyRegistry registry) {
        registry.add("app.storage.base-path", () -> storageDir.toString());
    }

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

    @Test
    void fullLifecycleWritesOrderedTranscriptFile() {
        publish("meeting.started", """
                { "event": "meeting.started",
                  "meeting": { "id": "%s", "sessionId": "%s", "title": "Q4",
                    "startedAt": "2024-12-13T06:57:09.736Z", "createdAt": "2024-12-13T06:57:09.736Z" } }
                """.formatted(meetingId, sessionId));

        publish("meeting.transcript", transcript(UUID.randomUUID(), 1, "Alice", "First line", 2, 5));
        publish("meeting.transcript", transcript(UUID.randomUUID(), 2, "Bob", "Second line", 6, 9));

        publish("meeting.ended", """
                { "event": "meeting.ended",
                  "meeting": { "id": "%s", "sessionId": "%s", "title": "Q4",
                    "endedAt": "2024-12-13T07:04:37.052Z" },
                  "reason": "HOST_ENDED_MEETING" }
                """.formatted(meetingId, sessionId));

        // The reconstruct consumer eventually writes the file and stamps the URI.
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Path file = storageDir.resolve(sessionId + ".txt");
            assertThat(Files.exists(file)).isTrue();
            String content = Files.readString(file);
            assertThat(content).isEqualTo(
                    "[2-5s] Alice: First line\n[6-9s] Bob: Second line\n");

            assertThat(sessionRepository.findById(sessionId))
                    .get()
                    .satisfies(s -> assertThat(s.getTranscriptUri()).contains(sessionId + ".txt"));
        });
    }

    private String transcript(UUID transcriptId, int seq, String speaker, String content,
                              int start, int end) {
        return """
                { "event": "meeting.transcript",
                  "meeting": { "id": "%s", "sessionId": "%s" },
                  "data": {
                    "transcriptId": "%s", "sequenceNumber": %d,
                    "speaker": { "id": "70c5d391-5bca-4cf3-9907-bec205798adb", "name": "%s" },
                    "content": "%s", "startOffset": "%d", "endOffset": "%d", "language": "en"
                  } }
                """.formatted(meetingId, sessionId, transcriptId, seq, speaker, content, start, end);
    }
}
