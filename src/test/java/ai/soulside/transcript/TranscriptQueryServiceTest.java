package ai.soulside.transcript;

import ai.soulside.common.web.ResourceNotFoundException;
import ai.soulside.meeting.model.Meeting;
import ai.soulside.meeting.model.Session;
import ai.soulside.meeting.model.SessionStatus;
import ai.soulside.meeting.repository.SessionRepository;
import ai.soulside.transcript.dto.TranscriptResponse;
import ai.soulside.transcript.model.TranscriptSegment;
import ai.soulside.transcript.repository.TranscriptSegmentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TranscriptQueryServiceTest {

    @Mock
    private SessionRepository sessionRepository;
    @Mock
    private TranscriptSegmentRepository segmentRepository;

    @InjectMocks
    private TranscriptQueryService service;

    private UUID meetingId;
    private UUID sessionId;

    @BeforeEach
    void setUp() {
        meetingId = UUID.randomUUID();
        sessionId = UUID.randomUUID();
    }

    private TranscriptSegment segment(int seq, String content) {
        return TranscriptSegment.builder()
                .transcriptId(UUID.randomUUID())
                .sequenceNumber(seq)
                .speakerId(UUID.randomUUID())
                .speakerName("Speaker " + seq)
                .content(content)
                .startOffset(seq)
                .endOffset(seq + 1)
                .language("en")
                .build();
    }

    @Test
    void returnsOrderedTranscriptWithMetadata() {
        Meeting meeting = Meeting.builder().id(meetingId).title("Q4 Sync")
                .createdAt(Instant.now()).build();
        Session session = Session.builder().id(sessionId).meeting(meeting)
                .status(SessionStatus.ENDED)
                .startedAt(Instant.parse("2024-12-13T06:57:09Z"))
                .endedAt(Instant.parse("2024-12-13T07:04:37Z"))
                .transcriptUri("file:///t.txt")
                .build();
        when(sessionRepository.findByIdAndMeetingId(sessionId, meetingId))
                .thenReturn(Optional.of(session));
        when(segmentRepository.findBySessionIdOrderBySequenceNumberAsc(sessionId))
                .thenReturn(List.of(segment(1, "first"), segment(2, "second")));

        TranscriptResponse response = service.getTranscript(meetingId, sessionId);

        assertThat(response.meetingId()).isEqualTo(meetingId);
        assertThat(response.meetingTitle()).isEqualTo("Q4 Sync");
        assertThat(response.sessionId()).isEqualTo(sessionId);
        assertThat(response.status()).isEqualTo(SessionStatus.ENDED);
        assertThat(response.transcriptUri()).isEqualTo("file:///t.txt");
        assertThat(response.segmentCount()).isEqualTo(2);
        assertThat(response.entries()).extracting("sequenceNumber").containsExactly(1, 2);
        assertThat(response.entries()).extracting("content").containsExactly("first", "second");
        assertThat(response.entries().get(0).startOffsetSeconds()).isEqualTo(1);
    }

    @Test
    void liveSessionWithNoSegmentsReturnsEmptyEntries() {
        Meeting meeting = Meeting.builder().id(meetingId).title("Live").createdAt(Instant.now()).build();
        Session session = Session.builder().id(sessionId).meeting(meeting)
                .status(SessionStatus.LIVE).startedAt(Instant.now()).build();
        when(sessionRepository.findByIdAndMeetingId(sessionId, meetingId))
                .thenReturn(Optional.of(session));
        when(segmentRepository.findBySessionIdOrderBySequenceNumberAsc(sessionId))
                .thenReturn(List.of());

        TranscriptResponse response = service.getTranscript(meetingId, sessionId);

        assertThat(response.status()).isEqualTo(SessionStatus.LIVE);
        assertThat(response.segmentCount()).isZero();
        assertThat(response.entries()).isEmpty();
        assertThat(response.transcriptUri()).isNull();
    }

    @Test
    void unknownSessionOrMeetingThrowsNotFound() {
        when(sessionRepository.findByIdAndMeetingId(sessionId, meetingId))
                .thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.getTranscript(meetingId, sessionId))
                .isInstanceOf(ResourceNotFoundException.class)
                .hasMessageContaining(sessionId.toString());

        verify(segmentRepository, never()).findBySessionIdOrderBySequenceNumberAsc(any());
    }
}
