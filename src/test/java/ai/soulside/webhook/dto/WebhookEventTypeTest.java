package ai.soulside.webhook.dto;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WebhookEventTypeTest {

    @Test
    void resolvesKnownWireValues() {
        assertThat(WebhookEventType.fromWireValue("meeting.started"))
                .isEqualTo(WebhookEventType.MEETING_STARTED);
        assertThat(WebhookEventType.fromWireValue("meeting.transcript"))
                .isEqualTo(WebhookEventType.MEETING_TRANSCRIPT);
        assertThat(WebhookEventType.fromWireValue("meeting.ended"))
                .isEqualTo(WebhookEventType.MEETING_ENDED);
    }

    @Test
    void returnsNullForUnknownOrNull() {
        assertThat(WebhookEventType.fromWireValue("meeting.exploded")).isNull();
        assertThat(WebhookEventType.fromWireValue(null)).isNull();
    }

    @Test
    void exposesWireValue() {
        assertThat(WebhookEventType.MEETING_STARTED.getWireValue()).isEqualTo("meeting.started");
    }
}
