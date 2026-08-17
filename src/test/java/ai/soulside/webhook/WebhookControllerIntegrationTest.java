package ai.soulside.webhook;

import ai.soulside.common.KafkaTopics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 3, topics = {KafkaTopics.MEETING_EVENTS})
class WebhookControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private Consumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> consumerProps =
                KafkaTestUtils.consumerProps("test-consumer", "true", embeddedKafka);
        consumer = new DefaultKafkaConsumerFactory<>(
                consumerProps, new StringDeserializer(), new StringDeserializer())
                .createConsumer();
        embeddedKafka.consumeFromEmbeddedTopics(consumer, KafkaTopics.MEETING_EVENTS);
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void validMeetingStartedReturns202AndPublishesToKafka() throws Exception {
        String sessionId = "05e57591-d89e-45c9-ae44-08dc1eaad0e0";
        String payload = """
                {
                  "event": "meeting.started",
                  "meeting": {
                    "id": "50c8940e-1b97-402a-97d6-2708b7feca41",
                    "sessionId": "%s",
                    "title": "Q4 Planning Sync",
                    "roomName": "lcfvaa-absxch",
                    "status": "LIVE",
                    "createdAt": "2024-12-13T06:57:09.736Z",
                    "startedAt": "2024-12-13T06:57:09.736Z",
                    "organizedBy": { "id": "70c5d391-5bca-4cf3-9907-bec205798adb", "name": "Alice" }
                  }
                }
                """.formatted(sessionId);

        mockMvc.perform(post("/api/v1/webhooks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isAccepted());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(consumer, KafkaTopics.MEETING_EVENTS);

        assertThat(record.key()).isEqualTo(sessionId);
        assertThat(record.value()).contains("meeting.started");
        assertThat(new String(record.headers().lastHeader("eventType").value(),
                StandardCharsets.UTF_8)).isEqualTo("meeting.started");
    }

    @Test
    void malformedJsonReturns400() throws Exception {
        mockMvc.perform(post("/api/v1/webhooks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{ this is not valid json "))
                .andExpect(status().isBadRequest());
    }

    @Test
    void missingRequiredFieldsReturns400() throws Exception {
        // Missing meeting.sessionId
        String payload = """
                {
                  "event": "meeting.started",
                  "meeting": { "id": "50c8940e-1b97-402a-97d6-2708b7feca41" }
                }
                """;

        mockMvc.perform(post("/api/v1/webhooks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isBadRequest());
    }

    @Test
    void unknownEventTypeReturns400() throws Exception {
        String payload = """
                {
                  "event": "meeting.exploded",
                  "meeting": {
                    "id": "50c8940e-1b97-402a-97d6-2708b7feca41",
                    "sessionId": "05e57591-d89e-45c9-ae44-08dc1eaad0e0"
                  }
                }
                """;

        mockMvc.perform(post("/api/v1/webhooks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isBadRequest());
    }
}
