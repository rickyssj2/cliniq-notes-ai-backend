package ai.soulside.common.config;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import tools.jackson.databind.exc.MismatchedInputException;

/**
 * Configures how the Kafka listener container reacts to processing failures.
 *
 * <p>Retryable failures (e.g. a transcript/end event arriving slightly before the session is
 * committed) are retried with exponential backoff. After the retries are exhausted — or
 * immediately for non-recoverable failures like malformed payloads — the record is published
 * to the {@code <topic>.DLT} dead-letter topic.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Value("${app.kafka.retry.initial-interval-ms:1000}")
    private long initialIntervalMs;
    @Value("${app.kafka.retry.multiplier:5.0}")
    private double multiplier;
    @Value("${app.kafka.retry.max-interval-ms:30000}")
    private long maxIntervalMs;
    @Value("${app.kafka.retry.max-attempts:3}")
    private long maxAttempts;

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> {
                    Throwable rootCause = rootCause(exception);
                    log.error("Publishing to DLQ. topic={} partition={} offset={} cause={}: {}",
                            record.topic(), record.partition(), record.offset(),
                            rootCause.getClass().getSimpleName(), rootCause.getMessage());
                    return new TopicPartition(record.topic() + ".DLT", record.partition());
                });

        // Exponential backoff (defaults ~1s, 5s, 30s capped, up to 3 retries); configurable.
        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(initialIntervalMs);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxIntervalMs);
        backOff.setMaxAttempts(maxAttempts);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

        // Deserialization / structural errors will never succeed on retry — DLQ immediately.
        handler.addNotRetryableExceptions(
                MismatchedInputException.class,
                IllegalArgumentException.class);

        return handler;
    }

    /** Unwrap listener/wrapper exceptions to the underlying cause for clearer DLQ triage. */
    private static Throwable rootCause(Throwable t) {
        Throwable current = t;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }
}
