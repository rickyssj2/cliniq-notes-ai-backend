package ai.soulside.meeting;

import ai.soulside.common.CorrelationId;
import ai.soulside.common.KafkaTopics;
import ai.soulside.meeting.event.MeetingEndedEvent;
import ai.soulside.meeting.event.MeetingStartedEvent;
import ai.soulside.webhook.dto.WebhookEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Consumes raw webhook payloads from {@code meeting.events} and routes them to the
 * appropriate handler based on the {@code eventType} header set by the producer.
 *
 * <p>Transcript events are handled in Phase 5; here we process the meeting lifecycle events.
 * Exceptions thrown from this method propagate to the container error handler, which applies
 * retry/backoff and eventually publishes to the DLQ.
 */
@Component
public class MeetingEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(MeetingEventConsumer.class);

    private final MeetingService meetingService;
    private final ObjectMapper objectMapper;

    public MeetingEventConsumer(MeetingService meetingService, ObjectMapper objectMapper) {
        this.meetingService = meetingService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = KafkaTopics.MEETING_EVENTS, groupId = "${spring.kafka.consumer.group-id}")
    public void consume(@Payload String rawPayload,
                        @Header(name = "eventType", required = false) String eventTypeHeader,
                        @Header(name = CorrelationId.HEADER, required = false) String correlationId,
                        @Header(KafkaHeaders.RECEIVED_KEY) String key) {

        if (correlationId != null) {
            MDC.put(CorrelationId.MDC_KEY, correlationId);
        }
        try {
            WebhookEventType eventType = WebhookEventType.fromWireValue(eventTypeHeader);
            if (eventType == null) {
                log.warn("Discarding event with unknown eventType header='{}' key={}",
                        eventTypeHeader, key);
                return;
            }

            switch (eventType) {
                case MEETING_STARTED -> meetingService.handleMeetingStarted(
                        objectMapper.readValue(rawPayload, MeetingStartedEvent.class));
                case MEETING_ENDED -> meetingService.handleMeetingEnded(
                        objectMapper.readValue(rawPayload, MeetingEndedEvent.class));
                case MEETING_TRANSCRIPT -> {
                    // Handled by the transcript consumer in Phase 5.
                    log.debug("Transcript event received; handled separately. key={}", key);
                }
            }
        } finally {
            MDC.remove(CorrelationId.MDC_KEY);
        }
    }
}
