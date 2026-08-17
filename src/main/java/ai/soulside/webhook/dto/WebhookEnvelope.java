package ai.soulside.webhook.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

/**
 * The common envelope shared by all webhook events.
 *
 * <p>Only the fields needed for validation and routing are bound here. The full,
 * event-specific body is deserialized downstream by the consumer. This keeps the
 * webhook endpoint fast and decoupled from the details of each event shape.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record WebhookEnvelope(

        @NotNull(message = "event is required")
        WebhookEventType event,

        @NotNull(message = "meeting is required")
        @Valid
        MeetingRef meeting
) {

    /**
     * Minimal meeting reference present in every event; used to derive the Kafka key.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record MeetingRef(

            @NotNull(message = "meeting.id is required")
            java.util.UUID id,

            @NotNull(message = "meeting.sessionId is required")
            java.util.UUID sessionId
    ) {
    }
}
