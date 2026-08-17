package ai.soulside.common.config;

import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Type-safe binding for application-specific configuration under the {@code app.*} prefix.
 */
@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "app")
public class AppProperties {

    private final Webhook webhook = new Webhook();
    private final Storage storage = new Storage();

    /**
     * Webhook ingestion settings.
     */
    @Getter
    @Setter
    public static class Webhook {

        /** Shared secret used to verify inbound HMAC-SHA256 signatures. */
        @NotBlank
        private String hmacSecret;
    }

    /**
     * Transcript storage settings.
     */
    @Getter
    @Setter
    public static class Storage {

        /** Root directory for assembled transcript files. */
        @NotBlank
        private String basePath;
    }
}
