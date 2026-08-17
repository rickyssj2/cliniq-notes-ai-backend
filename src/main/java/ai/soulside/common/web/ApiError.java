package ai.soulside.common.web;

import java.time.Instant;

/**
 * Standard error response body returned for 4xx/5xx responses.
 */
public record ApiError(
        Instant timestamp,
        int status,
        String error,
        String message
) {
    public static ApiError of(int status, String error, String message) {
        return new ApiError(Instant.now(), status, error, message);
    }
}
