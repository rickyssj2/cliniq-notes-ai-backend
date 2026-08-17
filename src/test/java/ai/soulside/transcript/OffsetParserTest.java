package ai.soulside.transcript;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OffsetParserTest {

    @Test
    void parsesPlainSeconds() {
        assertThat(OffsetParser.parseToSeconds("300")).isEqualTo(300);
        assertThat(OffsetParser.parseToSeconds("0")).isEqualTo(0);
    }

    @Test
    void truncatesFractionalSeconds() {
        assertThat(OffsetParser.parseToSeconds("300.9")).isEqualTo(300);
    }

    @Test
    void parsesClockFormat() {
        assertThat(OffsetParser.parseToSeconds("00:00:02.100")).isEqualTo(2);
        assertThat(OffsetParser.parseToSeconds("00:05:00")).isEqualTo(300);
        assertThat(OffsetParser.parseToSeconds("01:00:00")).isEqualTo(3600);
        assertThat(OffsetParser.parseToSeconds("01:02:03")).isEqualTo(3723);
    }

    @Test
    void returnsNullForNullOrBlank() {
        assertThat(OffsetParser.parseToSeconds(null)).isNull();
        assertThat(OffsetParser.parseToSeconds("   ")).isNull();
    }

    @Test
    void throwsForUnparseableValue() {
        assertThatThrownBy(() -> OffsetParser.parseToSeconds("not-a-number"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> OffsetParser.parseToSeconds("00:00"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
