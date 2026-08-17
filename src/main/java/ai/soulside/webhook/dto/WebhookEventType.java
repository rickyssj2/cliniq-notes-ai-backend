package ai.soulside.webhook.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;

/**
 * Supported webhook event types, mapped from the {@code event} field of the payload.
 */
public enum WebhookEventType {

    MEETING_STARTED("meeting.started"),
    MEETING_TRANSCRIPT("meeting.transcript"),
    MEETING_ENDED("meeting.ended");

    private final String wireValue;

    WebhookEventType(String wireValue) {
        this.wireValue = wireValue;
    }

    @JsonValue
    public String getWireValue() {
        return wireValue;
    }

    /**
     * Resolve the enum from its wire value. Returns {@code null} for unknown values
     * so validation can produce a meaningful error rather than a deserialization failure.
     */
    @JsonCreator
    public static WebhookEventType fromWireValue(String value) {
        if (value == null) {
            return null;
        }
        return Arrays.stream(values())
                .filter(t -> t.wireValue.equals(value))
                .findFirst()
                .orElse(null);
    }
}
