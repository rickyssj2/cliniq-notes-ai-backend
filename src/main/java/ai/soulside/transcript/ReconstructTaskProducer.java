package ai.soulside.transcript;

import ai.soulside.common.CorrelationId;
import ai.soulside.common.KafkaTopics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

/**
 * Publishes a task to {@code transcript.reconstruct} signaling that a session's transcript
 * should be assembled. The message body is the sessionId; the key is also the sessionId so
 * reconstruct tasks for a session stay ordered.
 */
@Service
public class ReconstructTaskProducer {

    private static final Logger log = LoggerFactory.getLogger(ReconstructTaskProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public ReconstructTaskProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void requestReconstruction(String sessionId) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(KafkaTopics.TRANSCRIPT_RECONSTRUCT, sessionId, sessionId);

        String correlationId = MDC.get(CorrelationId.MDC_KEY);
        if (correlationId != null) {
            record.headers().add(new RecordHeader(
                    CorrelationId.HEADER, correlationId.getBytes(StandardCharsets.UTF_8)));
        }

        kafkaTemplate.send(record);
        log.debug("Requested transcript reconstruction for sessionId={}", sessionId);
    }
}
