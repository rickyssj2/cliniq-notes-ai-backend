package ai.soulside.transcript;

/**
 * Parses transcript offset values into whole seconds relative to the session start.
 *
 * <p>The upstream payload is inconsistent about format:
 * <ul>
 *   <li>plain seconds — {@code "300"} or {@code "300.5"}</li>
 *   <li>clock offset — {@code "HH:MM:SS"} or {@code "HH:MM:SS.mmm"} (e.g. {@code "00:00:02.100"})</li>
 * </ul>
 * Both are normalized to an integer number of seconds (truncated, not rounded).
 */
public final class OffsetParser {

    private OffsetParser() {
    }

    /**
     * Parse an offset string into whole seconds.
     *
     * @param raw the offset value (plain seconds or HH:MM:SS[.mmm])
     * @return offset in seconds, or {@code null} if the input is null/blank
     * @throws IllegalArgumentException if the value cannot be parsed
     */
    public static Integer parseToSeconds(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String value = raw.trim();

        try {
            if (value.contains(":")) {
                return parseClock(value);
            }
            // Plain seconds, possibly with a fractional part.
            return (int) Math.floor(Double.parseDouble(value));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Unparseable offset value: " + raw, e);
        }
    }

    private static int parseClock(String value) {
        String[] parts = value.split(":");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Expected HH:MM:SS[.mmm], got: " + value);
        }
        int hours = Integer.parseInt(parts[0]);
        int minutes = Integer.parseInt(parts[1]);
        double seconds = Double.parseDouble(parts[2]);
        return hours * 3600 + minutes * 60 + (int) Math.floor(seconds);
    }
}
