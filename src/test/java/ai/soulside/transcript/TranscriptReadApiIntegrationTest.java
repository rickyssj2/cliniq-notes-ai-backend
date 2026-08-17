package ai.soulside.transcript;

import ai.soulside.common.KafkaTopics;
import ai.soulside.meeting.repository.MeetingRepository;
import ai.soulside.meeting.repository.SessionRepository;
import ai.soulside.transcript.repository.TranscriptSegmentRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {KafkaTopics.MEETING_EVENTS, KafkaTopics.TRANSCRIPT_RECONSTRUCT})
class TranscriptReadApiIntegrationTest {

    @Autowired
    private MockMvc mockMvc;
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
    void fullSimulationThenGetReturnsCompleteOrderedTranscript() throws Exception {
        publish("meeting.started", """
                { "event": "meeting.started",
                  "meeting": { "id": "%s", "sessionId": "%s", "title": "Q4 Planning",
                    "startedAt": "2024-12-13T06:57:09.736Z", "createdAt": "2024-12-13T06:57:09.736Z" } }
                """.formatted(meetingId, sessionId));

        publish("meeting.transcript", transcript(UUID.randomUUID(), 1, "Alice", "Let us begin", 2, 5));
        publish("meeting.transcript", transcript(UUID.randomUUID(), 2, "Bob", "Revenue is up", 6, 9));
        publish("meeting.transcript", transcript(UUID.randomUUID(), 3, "Alice", "Great news", 10, 12));

        publish("meeting.ended", """
                { "event": "meeting.ended",
                  "meeting": { "id": "%s", "sessionId": "%s", "title": "Q4 Planning",
                    "endedAt": "2024-12-13T07:04:37.052Z" },
                  "reason": "HOST_ENDED_MEETING" }
                """.formatted(meetingId, sessionId));

        // Wait until all three segments have been persisted.
        await().atMost(Duration.ofSeconds(20)).until(() ->
                segmentRepository.findBySessionIdOrderBySequenceNumberAsc(sessionId).size() == 3);

        mockMvc.perform(get("/api/v1/meetings/{m}/sessions/{s}/transcript", meetingId, sessionId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.meetingId").value(meetingId.toString()))
                .andExpect(jsonPath("$.sessionId").value(sessionId.toString()))
                .andExpect(jsonPath("$.status").value("ENDED"))
                .andExpect(jsonPath("$.segmentCount").value(3))
                .andExpect(jsonPath("$.entries.length()").value(3))
                .andExpect(jsonPath("$.entries[0].sequenceNumber").value(1))
                .andExpect(jsonPath("$.entries[0].content").value("Let us begin"))
                .andExpect(jsonPath("$.entries[0].speakerName").value("Alice"))
                .andExpect(jsonPath("$.entries[0].startOffsetSeconds").value(2))
                .andExpect(jsonPath("$.entries[1].sequenceNumber").value(2))
                .andExpect(jsonPath("$.entries[2].sequenceNumber").value(3))
                .andExpect(jsonPath("$.entries[2].content").value("Great news"));
    }

    @Test
    void unknownSessionReturns404() throws Exception {
        mockMvc.perform(get("/api/v1/meetings/{m}/sessions/{s}/transcript",
                        UUID.randomUUID(), UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.message").exists());
    }

    @Test
    void liveSessionReturnsOrderedSegmentsBeforeEnd() throws Exception {
        publish("meeting.started", """
                { "event": "meeting.started",
                  "meeting": { "id": "%s", "sessionId": "%s", "title": "Live",
                    "startedAt": "2024-12-13T06:57:09.736Z", "createdAt": "2024-12-13T06:57:09.736Z" } }
                """.formatted(meetingId, sessionId));
        publish("meeting.transcript", transcript(UUID.randomUUID(), 1, "Alice", "Ongoing", 0, 3));

        await().atMost(Duration.ofSeconds(20)).until(() ->
                !segmentRepository.findBySessionIdOrderBySequenceNumberAsc(sessionId).isEmpty());

        mockMvc.perform(get("/api/v1/meetings/{m}/sessions/{s}/transcript", meetingId, sessionId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("LIVE"))
                .andExpect(jsonPath("$.entries[0].content").value("Ongoing"));
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
