package ai.soulside.webhook;

import ai.soulside.webhook.dto.WebhookEnvelope;
import ai.soulside.webhook.security.HmacSignatureVerifier;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Receives meeting webhook events, verifies their signature, validates the envelope,
 * and hands them off to Kafka for asynchronous processing.
 *
 * <p>The raw body is consumed directly (rather than via {@code @RequestBody} binding) so the
 * exact bytes can be fed to HMAC verification before any parsing, and forwarded verbatim to Kafka.
 */
@RestController
@RequestMapping("/api/v1/webhooks")
public class WebhookController {

    private static final Logger log = LoggerFactory.getLogger(WebhookController.class);

    private final HmacSignatureVerifier signatureVerifier;
    private final KafkaProducerService producerService;
    private final ObjectMapper objectMapper;
    private final Validator validator;

    public WebhookController(HmacSignatureVerifier signatureVerifier,
                             KafkaProducerService producerService,
                             ObjectMapper objectMapper,
                             Validator validator) {
        this.signatureVerifier = signatureVerifier;
        this.producerService = producerService;
        this.objectMapper = objectMapper;
        this.validator = validator;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void receive(@RequestBody byte[] rawBody,
                        @RequestHeader(value = HmacSignatureVerifier.SIGNATURE_HEADER,
                                required = false) String signature) {

        // 1. Verify signature over the raw bytes (fail-closed when enabled).
        signatureVerifier.verify(rawBody, signature);

        // 2. Parse and validate the common envelope.
        WebhookEnvelope envelope = parseEnvelope(rawBody);
        validateEnvelope(envelope);

        // 3. Publish the raw payload to Kafka, keyed by sessionId.
        String sessionId = envelope.meeting().sessionId().toString();
        String rawPayload = new String(rawBody, StandardCharsets.UTF_8);
        producerService.publish(sessionId, envelope.event(), rawPayload);

        log.info("Accepted webhook event={} meetingId={} sessionId={}",
                envelope.event().getWireValue(), envelope.meeting().id(), sessionId);
    }

    private WebhookEnvelope parseEnvelope(byte[] rawBody) {
        try {
            return objectMapper.readValue(rawBody, WebhookEnvelope.class);
        } catch (Exception e) {
            throw new InvalidWebhookPayloadException("Malformed webhook payload: " + e.getMessage());
        }
    }

    private void validateEnvelope(WebhookEnvelope envelope) {
        Set<ConstraintViolation<WebhookEnvelope>> violations = validator.validate(envelope);
        if (!violations.isEmpty()) {
            String message = violations.stream()
                    .map(v -> v.getPropertyPath() + " " + v.getMessage())
                    .sorted()
                    .collect(Collectors.joining("; "));
            throw new InvalidWebhookPayloadException(message);
        }
    }
}
