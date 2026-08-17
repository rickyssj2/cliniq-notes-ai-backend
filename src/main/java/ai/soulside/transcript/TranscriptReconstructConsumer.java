package ai.soulside.transcript;

import ai.soulside.common.CorrelationId;
import ai.soulside.common.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Consumes reconstruction tasks from {@code transcript.reconstruct} and assembles the transcript
 * for the referenced session. The message body is the sessionId.
 *
 * <p>Failures propagate to the container error handler for retry/backoff and eventual DLQ.
 */
@Component
public class TranscriptReconstructConsumer {

    private static final Logger log = LoggerFactory.getLogger(TranscriptReconstructConsumer.class);

    private final TranscriptReconstructionService reconstructionService;

    public TranscriptReconstructConsumer(TranscriptReconstructionService reconstructionService) {
        this.reconstructionService = reconstructionService;
    }

    @KafkaListener(topics = KafkaTopics.TRANSCRIPT_RECONSTRUCT,
            groupId = "${spring.kafka.consumer.group-id}")
    public void consume(@Payload String sessionId,
                        @Header(name = CorrelationId.HEADER, required = false) String correlationId) {
        if (correlationId != null) {
            MDC.put(CorrelationId.MDC_KEY, correlationId);
        }
        try {
            log.debug("Reconstruction task received for sessionId={}", sessionId);
            reconstructionService.reconstruct(UUID.fromString(sessionId));
        } finally {
            MDC.remove(CorrelationId.MDC_KEY);
        }
    }
}
