package ai.soulside.meeting;

import ai.soulside.common.KafkaTopics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
 * A structurally invalid transcript (missing {@code data}) should fail fast with a non-retryable
 * {@link IllegalArgumentException} and dead-letter promptly, rather than exhausting the retry cycle.
 */
@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {
        KafkaTopics.MEETING_EVENTS,
        KafkaTopics.MEETING_EVENTS_DLT,
        KafkaTopics.TRANSCRIPT_RECONSTRUCT
})
class MalformedTranscriptDlqIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private Consumer<String, String> dltConsumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> props =
                KafkaTestUtils.consumerProps("malformed-dlt-consumer", "true", embeddedKafka);
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
    void malformedTranscriptDeadLettersFast() {
        UUID sessionId = UUID.randomUUID();
        // Valid envelope + eventType, but no "data" block — will NPE without the guard.
        String payload = """
                { "event": "meeting.transcript",
                  "meeting": { "id": "%s", "sessionId": "%s" } }
                """.formatted(UUID.randomUUID(), sessionId);

        ProducerRecord<String, String> record =
                new ProducerRecord<>(KafkaTopics.MEETING_EVENTS, sessionId.toString(), payload);
        record.headers().add(new RecordHeader("eventType",
                "meeting.transcript".getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(record);

        long start = System.currentTimeMillis();
        ConsumerRecord<String, String> dltRecord = awaitDlqRecordForKey(sessionId.toString());
        long elapsed = System.currentTimeMillis() - start;

        assertThat(dltRecord.key()).isEqualTo(sessionId.toString());
        // Non-retryable => should dead-letter well before the retry budget would elapse.
        assertThat(elapsed).isLessThan(5_000);
    }

    private ConsumerRecord<String, String> awaitDlqRecordForKey(String key) {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(10).toMillis();
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records =
                    KafkaTestUtils.getRecords(dltConsumer, Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> r : records) {
                if (key.equals(r.key())) {
                    return r;
                }
            }
        }
        throw new AssertionError("No DLQ record for key " + key + " within timeout");
    }
}
