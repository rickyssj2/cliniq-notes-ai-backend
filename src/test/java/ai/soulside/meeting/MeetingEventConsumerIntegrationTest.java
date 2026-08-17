package ai.soulside.meeting;

import ai.soulside.common.KafkaTopics;
import ai.soulside.meeting.model.SessionStatus;
import ai.soulside.meeting.repository.MeetingRepository;
import ai.soulside.meeting.repository.SessionRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.junit.jupiter.api.BeforeEach;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {KafkaTopics.MEETING_EVENTS, KafkaTopics.TRANSCRIPT_RECONSTRUCT})
class MeetingEventConsumerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private MeetingRepository meetingRepository;
    @Autowired
    private SessionRepository sessionRepository;

    @BeforeEach
    void cleanDatabase() {
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

    @Test
    void meetingStartedCreatesMeetingAndLiveSession() {
        UUID meetingId = UUID.randomUUID();
        UUID sessionId = UUID.randomUUID();
        String payload = """
                {
                  "event": "meeting.started",
                  "meeting": {
                    "id": "%s", "sessionId": "%s",
                    "title": "Q4 Planning", "roomName": "room-1", "status": "LIVE",
                    "createdAt": "2024-12-13T06:57:09.736Z",
                    "startedAt": "2024-12-13T06:57:09.736Z",
                    "organizedBy": { "id": "70c5d391-5bca-4cf3-9907-bec205798adb", "name": "Alice" }
                  }
                }
                """.formatted(meetingId, sessionId);

        publish(sessionId.toString(), "meeting.started", payload);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(meetingRepository.findById(meetingId)).isPresent();
            assertThat(sessionRepository.findById(sessionId))
                    .isPresent()
                    .get()
                    .satisfies(s -> assertThat(s.getStatus()).isEqualTo(SessionStatus.LIVE));
        });
    }

    @Test
    void meetingEndedMarksSessionEnded() {
        UUID meetingId = UUID.randomUUID();
        UUID sessionId = UUID.randomUUID();

        String started = """
                { "event": "meeting.started",
                  "meeting": { "id": "%s", "sessionId": "%s", "title": "T",
                    "startedAt": "2024-12-13T06:57:09.736Z", "createdAt": "2024-12-13T06:57:09.736Z" } }
                """.formatted(meetingId, sessionId);
        publish(sessionId.toString(), "meeting.started", started);

        await().atMost(Duration.ofSeconds(10))
                .until(() -> sessionRepository.findById(sessionId).isPresent());

        String ended = """
                { "event": "meeting.ended",
                  "meeting": { "id": "%s", "sessionId": "%s", "title": "T",
                    "endedAt": "2024-12-13T07:04:37.052Z" },
                  "reason": "HOST_ENDED_MEETING" }
                """.formatted(meetingId, sessionId);
        publish(sessionId.toString(), "meeting.ended", ended);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(sessionRepository.findById(sessionId))
                        .get()
                        .satisfies(s -> assertThat(s.getStatus()).isEqualTo(SessionStatus.ENDED)));
    }

    @Test
    void duplicateMeetingStartedDoesNotCreateSecondSession() {
        UUID meetingId = UUID.randomUUID();
        UUID sessionId = UUID.randomUUID();
        String payload = """
                { "event": "meeting.started",
                  "meeting": { "id": "%s", "sessionId": "%s", "title": "Dup",
                    "startedAt": "2024-12-13T06:57:09.736Z", "createdAt": "2024-12-13T06:57:09.736Z" } }
                """.formatted(meetingId, sessionId);

        publish(sessionId.toString(), "meeting.started", payload);
        publish(sessionId.toString(), "meeting.started", payload);

        await().atMost(Duration.ofSeconds(10))
                .until(() -> sessionRepository.findById(sessionId).isPresent());

        // Both deliveries share the same sessionId (the PK), so a duplicate can never create
        // a second row. Confirm exactly one session exists after both are processed.
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                assertThat(sessionRepository.count()).isEqualTo(1));
    }
}
