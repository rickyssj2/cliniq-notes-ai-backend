package ai.soulside.meeting;

import ai.soulside.common.KafkaTopics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that an event which keeps failing (meeting.ended for a session that was never
 * started) is retried and eventually published to the dead-letter topic.
 */
@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {
        KafkaTopics.MEETING_EVENTS,
        KafkaTopics.MEETING_EVENTS_DLT,
        KafkaTopics.TRANSCRIPT_RECONSTRUCT
})
class DeadLetterIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private Consumer<String, String> dltConsumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> props =
                KafkaTestUtils.consumerProps("dlt-consumer", "true", embeddedKafka);
        dltConsumer = new DefaultKafkaConsumerFactory<>(
                props, new StringDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(dltConsumer, KafkaTopics.MEETING_EVENTS_DLT);
    }

    @AfterEach
    void tearDown() {
        if (dltConsumer != null) {
            dltConsumer.close();
        }
    }

    @Test
    void meetingEndedForUnknownSessionLandsInDlq() {
        UUID sessionId = UUID.randomUUID();
        String payload = """
                { "event": "meeting.ended",
                  "meeting": { "id": "%s", "sessionId": "%s", "title": "Ghost",
                    "endedAt": "2024-12-13T07:04:37.052Z" },
                  "reason": "HOST_ENDED_MEETING" }
                """.formatted(UUID.randomUUID(), sessionId);

        ProducerRecord<String, String> record =
                new ProducerRecord<>(KafkaTopics.MEETING_EVENTS, sessionId.toString(), payload);
        record.headers().add(new RecordHeader("eventType",
                "meeting.ended".getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(record);

        // Retries with backoff (1s, 5s, ...) then DLQ; allow generous time.
        ConsumerRecord<String, String> dltRecord =
                KafkaTestUtils.getSingleRecord(dltConsumer, KafkaTopics.MEETING_EVENTS_DLT,
                        Duration.ofSeconds(30));

        assertThat(dltRecord.value()).contains(sessionId.toString());
        assertThat(dltRecord.key()).isEqualTo(sessionId.toString());
    }
}
