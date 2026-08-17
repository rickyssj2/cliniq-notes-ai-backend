package ai.soulside.webhook;

import ai.soulside.common.CorrelationId;
import ai.soulside.common.KafkaTopics;
import ai.soulside.webhook.dto.WebhookEventType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

/**
 * Publishes inbound webhook payloads to the {@code meeting.events} topic.
 *
 * <p>Messages are keyed by {@code sessionId} so that all events for a session land on the
 * same partition and are processed in order. The correlation id and event type are attached
 * as headers for downstream tracing and routing.
 */
@Service
public class KafkaProducerService {

    private static final String HEADER_EVENT_TYPE = "eventType";
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publish a raw webhook payload keyed by session id.
     *
     * @param sessionId  partition key — all events for a session are ordered
     * @param eventType  the event type, attached as a header for routing
     * @param rawPayload the raw JSON body as received
     */
    public void publish(String sessionId, WebhookEventType eventType, String rawPayload) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(KafkaTopics.MEETING_EVENTS, sessionId, rawPayload);

        record.headers().add(new RecordHeader(
                HEADER_EVENT_TYPE, eventType.getWireValue().getBytes(StandardCharsets.UTF_8)));

        String correlationId = MDC.get(CorrelationId.MDC_KEY);
        if (correlationId != null) {
            record.headers().add(new RecordHeader(
                    CorrelationId.HEADER, correlationId.getBytes(StandardCharsets.UTF_8)));
        }

        kafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish event type={} sessionId={}", eventType, sessionId, ex);
            } else {
                log.debug("Published event type={} sessionId={} partition={} offset={}",
                        eventType, sessionId,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
