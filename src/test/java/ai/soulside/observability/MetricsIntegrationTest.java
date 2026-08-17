package ai.soulside.observability;

import ai.soulside.common.KafkaTopics;
import ai.soulside.meeting.repository.SessionRepository;
import ai.soulside.transcript.repository.TranscriptSegmentRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Verifies custom Micrometer metrics are exposed on {@code /actuator/prometheus} after processing.
 */
@SpringBootTest(properties = "management.endpoints.web.exposure.include=health,info,prometheus,metrics")
@AutoConfigureMockMvc
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {KafkaTopics.MEETING_EVENTS, KafkaTopics.TRANSCRIPT_RECONSTRUCT})
class MetricsIntegrationTest {

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private SessionRepository sessionRepository;
    @Autowired
    private TranscriptSegmentRepository segmentRepository;

    @Test
    void customMetricsExposedOnPrometheusEndpoint() throws Exception {
        UUID meetingId = UUID.randomUUID();
        UUID sessionId = UUID.randomUUID();

        // Drive a webhook (increments webhook.events.received) which flows through the consumer.
        String started = """
                { "event": "meeting.started",
                  "meeting": { "id": "%s", "sessionId": "%s", "title": "Metrics",
                    "startedAt": "2024-12-13T06:57:09.736Z", "createdAt": "2024-12-13T06:57:09.736Z" } }
                """.formatted(meetingId, sessionId);

        mockMvc.perform(post("/api/v1/webhooks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(started))
                .andExpect(status().isAccepted());

        // Also publish an end event directly so reconstruction runs.
        publish(sessionId, "meeting.transcript", """
                { "event": "meeting.transcript",
                  "meeting": { "id": "%s", "sessionId": "%s" },
                  "data": { "transcriptId": "%s", "sequenceNumber": 1,
                    "speaker": { "id": "70c5d391-5bca-4cf3-9907-bec205798adb", "name": "Alice" },
                    "content": "hi", "startOffset": "0", "endOffset": "2", "language": "en" } }
                """.formatted(meetingId, sessionId, UUID.randomUUID()));
        publish(sessionId, "meeting.ended", """
                { "event": "meeting.ended",
                  "meeting": { "id": "%s", "sessionId": "%s", "endedAt": "2024-12-13T07:04:37.052Z" },
                  "reason": "HOST_ENDED_MEETING" }
                """.formatted(meetingId, sessionId));

        // Wait for async processing to reach reconstruction.
        await().atMost(Duration.ofSeconds(20)).until(() ->
                sessionRepository.findById(sessionId)
                        .map(s -> s.getTranscriptUri() != null).orElse(false));

        mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.containsString("webhook_events_received")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("consumer_event_processing_time")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("transcript_reconstruction_count")));
    }

    private void publish(UUID sessionId, String eventType, String payload) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(KafkaTopics.MEETING_EVENTS, sessionId.toString(), payload);
        record.headers().add(new RecordHeader("eventType",
                eventType.getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(record);
    }
}
