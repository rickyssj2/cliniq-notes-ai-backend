package ai.soulside.meeting;

import ai.soulside.common.CorrelationId;
import ai.soulside.common.KafkaTopics;
import ai.soulside.common.LogFields;
import ai.soulside.common.Metrics;
import ai.soulside.meeting.event.MeetingEndedEvent;
import ai.soulside.meeting.event.MeetingStartedEvent;
import ai.soulside.transcript.TranscriptService;
import ai.soulside.transcript.event.MeetingTranscriptEvent;
import ai.soulside.webhook.dto.WebhookEventType;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
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
    private final TranscriptService transcriptService;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    public MeetingEventConsumer(MeetingService meetingService,
                                TranscriptService transcriptService,
                                ObjectMapper objectMapper,
                                MeterRegistry meterRegistry) {
        this.meetingService = meetingService;
        this.transcriptService = transcriptService;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
    }

    @KafkaListener(topics = KafkaTopics.MEETING_EVENTS, groupId = "${spring.kafka.consumer.group-id}")
    public void consume(@Payload String rawPayload,
                        @Header(name = "eventType", required = false) String eventTypeHeader,
                        @Header(name = CorrelationId.HEADER, required = false) String correlationId,
                        @Header(KafkaHeaders.RECEIVED_KEY) String key) {

        if (correlationId != null) {
            MDC.put(CorrelationId.MDC_KEY, correlationId);
        }
        MDC.put(LogFields.SESSION_ID, key);
        if (eventTypeHeader != null) {
            MDC.put(LogFields.EVENT, eventTypeHeader);
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        String outcome = Metrics.OUTCOME_SUCCESS;
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
                case MEETING_TRANSCRIPT -> transcriptService.handleTranscript(
                        objectMapper.readValue(rawPayload, MeetingTranscriptEvent.class));
            }
        } catch (RuntimeException e) {
            // Record the failure metric, then rethrow so the error handler retries/dead-letters.
            outcome = Metrics.OUTCOME_FAILURE;
            throw e;
        } finally {
            String eventTag = eventTypeHeader != null ? eventTypeHeader : "unknown";
            sample.stop(meterRegistry.timer(Metrics.CONSUMER_PROCESSING_TIME, Metrics.TAG_EVENT, eventTag));
            meterRegistry.counter(Metrics.CONSUMER_EVENTS_PROCESSED,
                    Metrics.TAG_EVENT, eventTag, Metrics.TAG_OUTCOME, outcome).increment();
            MDC.remove(CorrelationId.MDC_KEY);
            MDC.remove(LogFields.SESSION_ID);
            MDC.remove(LogFields.EVENT);
        }
    }
}
