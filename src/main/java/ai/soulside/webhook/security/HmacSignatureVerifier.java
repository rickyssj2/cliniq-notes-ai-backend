package ai.soulside.webhook.security;

import ai.soulside.common.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;

/**
 * Verifies the {@code X-Signature} header of inbound webhooks using HMAC-SHA256
 * over the raw request body.
 *
 * <p>The comparison is constant-time to avoid timing side channels. When verification
 * is disabled via configuration, all requests pass — intended for local development only.
 */
@Component
public class HmacSignatureVerifier {

    /** Header carrying the hex-encoded HMAC-SHA256 of the request body. */
    public static final String SIGNATURE_HEADER = "X-Signature";

    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final Logger log = LoggerFactory.getLogger(HmacSignatureVerifier.class);

    private final AppProperties.Webhook config;

    public HmacSignatureVerifier(AppProperties appProperties) {
        this.config = appProperties.getWebhook();
    }

    /**
     * Verify the provided signature against the raw payload.
     *
     * @param rawBody           raw request body bytes (exactly as received)
     * @param providedSignature value of the {@code X-Signature} header (may be null)
     * @throws SignatureVerificationException if verification is enabled and the signature
     *                                        is missing or does not match
     */
    public void verify(byte[] rawBody, String providedSignature) {
        if (!config.isSignatureVerificationEnabled()) {
            log.debug("Signature verification disabled; skipping.");
            return;
        }

        if (!StringUtils.hasText(providedSignature)) {
            throw new SignatureVerificationException("Missing " + SIGNATURE_HEADER + " header");
        }

        String expected = computeSignature(rawBody);
        if (!constantTimeEquals(expected, providedSignature)) {
            throw new SignatureVerificationException("Signature mismatch");
        }
    }

    /**
     * Compute the hex-encoded HMAC-SHA256 signature for a payload. Exposed for testing
     * and for clients that need to generate a matching signature.
     */
    public String computeSignature(byte[] rawBody) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(
                    config.getHmacSecret().getBytes(StandardCharsets.UTF_8), HMAC_ALGORITHM));
            byte[] digest = mac.doFinal(rawBody);
            return HexFormat.of().formatHex(digest);
        } catch (Exception e) {
            // A misconfigured secret/algorithm is a server fault, not a client 401.
            throw new IllegalStateException("Failed to compute HMAC signature", e);
        }
    }

    private boolean constantTimeEquals(String a, String b) {
        return MessageDigest.isEqual(
                a.getBytes(StandardCharsets.UTF_8),
                b.getBytes(StandardCharsets.UTF_8));
    }
}
