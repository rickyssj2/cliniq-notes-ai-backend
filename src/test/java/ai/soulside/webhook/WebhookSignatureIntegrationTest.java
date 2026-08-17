package ai.soulside.webhook;

import ai.soulside.common.KafkaTopics;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Verifies fail-closed behavior: when signature verification is enabled, an unsigned
 * request is rejected with 401.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {KafkaTopics.MEETING_EVENTS})
@TestPropertySource(properties = {
        "app.webhook.signature-verification-enabled=true",
        "app.webhook.hmac-secret=test-secret"
})
class WebhookSignatureIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void unsignedRequestReturns401WhenVerificationEnabled() throws Exception {
        String payload = """
                {
                  "event": "meeting.started",
                  "meeting": {
                    "id": "50c8940e-1b97-402a-97d6-2708b7feca41",
                    "sessionId": "05e57591-d89e-45c9-ae44-08dc1eaad0e0"
                  }
                }
                """;

        mockMvc.perform(post("/api/v1/webhooks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isUnauthorized());
    }
}
