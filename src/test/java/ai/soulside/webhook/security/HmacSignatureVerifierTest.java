package ai.soulside.webhook.security;

import ai.soulside.common.config.AppProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HmacSignatureVerifierTest {

    private AppProperties props;

    @BeforeEach
    void setUp() {
        props = new AppProperties();
        props.getWebhook().setHmacSecret("super-secret-key");
        props.getWebhook().setSignatureVerificationEnabled(true);
    }

    @Test
    void acceptsValidSignature() {
        HmacSignatureVerifier verifier = new HmacSignatureVerifier(props);
        byte[] body = "{\"event\":\"meeting.started\"}".getBytes(StandardCharsets.UTF_8);
        String signature = verifier.computeSignature(body);

        assertThatCode(() -> verifier.verify(body, signature)).doesNotThrowAnyException();
    }

    @Test
    void rejectsMissingSignature() {
        HmacSignatureVerifier verifier = new HmacSignatureVerifier(props);
        byte[] body = "{}".getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> verifier.verify(body, null))
                .isInstanceOf(SignatureVerificationException.class)
                .hasMessageContaining("Missing");
    }

    @Test
    void rejectsTamperedBody() {
        HmacSignatureVerifier verifier = new HmacSignatureVerifier(props);
        String signature = verifier.computeSignature("original".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() ->
                verifier.verify("tampered".getBytes(StandardCharsets.UTF_8), signature))
                .isInstanceOf(SignatureVerificationException.class)
                .hasMessageContaining("mismatch");
    }

    @Test
    void skipsVerificationWhenDisabled() {
        props.getWebhook().setSignatureVerificationEnabled(false);
        HmacSignatureVerifier verifier = new HmacSignatureVerifier(props);

        assertThatCode(() -> verifier.verify("anything".getBytes(StandardCharsets.UTF_8), null))
                .doesNotThrowAnyException();
    }
}
