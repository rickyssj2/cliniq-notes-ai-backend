package ai.soulside.transcript;

import ai.soulside.meeting.UnknownSessionException;
import ai.soulside.meeting.model.Session;
import ai.soulside.meeting.model.SessionStatus;
import ai.soulside.meeting.repository.SessionRepository;
import ai.soulside.transcript.event.MeetingTranscriptEvent;
import ai.soulside.transcript.model.TranscriptSegment;
import ai.soulside.transcript.repository.TranscriptSegmentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TranscriptServiceTest {

    @Mock
    private SessionRepository sessionRepository;
    @Mock
    private TranscriptSegmentRepository segmentRepository;

    @InjectMocks
    private TranscriptService transcriptService;

    private UUID sessionId;
    private UUID transcriptId;

    @BeforeEach
    void setUp() {
        sessionId = UUID.randomUUID();
        transcriptId = UUID.randomUUID();
    }

    private MeetingTranscriptEvent event(int seq) {
        return new MeetingTranscriptEvent(
                "meeting.transcript",
                new MeetingTranscriptEvent.MeetingRef(UUID.randomUUID(), sessionId),
                new MeetingTranscriptEvent.TranscriptData(
                        transcriptId, seq,
                        new MeetingTranscriptEvent.Speaker(UUID.randomUUID(), "Alice"),
                        "Hello world", "00:00:00", "00:00:05", "en"));
    }

    @Test
    void persistsSegmentWhenSessionExists() {
        Session session = Session.builder().id(sessionId)
                .status(SessionStatus.LIVE).startedAt(Instant.now()).build();
        when(segmentRepository.existsByTranscriptId(transcriptId)).thenReturn(false);
        when(sessionRepository.findById(sessionId)).thenReturn(Optional.of(session));

        transcriptService.handleTranscript(event(1));

        ArgumentCaptor<TranscriptSegment> captor = ArgumentCaptor.forClass(TranscriptSegment.class);
        verify(segmentRepository).save(captor.capture());
        TranscriptSegment saved = captor.getValue();
        assertThat(saved.getTranscriptId()).isEqualTo(transcriptId);
        assertThat(saved.getSequenceNumber()).isEqualTo(1);
        assertThat(saved.getContent()).isEqualTo("Hello world");
        assertThat(saved.getSpeakerName()).isEqualTo("Alice");
        assertThat(saved.getLanguage()).isEqualTo("en");
    }

    @Test
    void skipsDuplicateTranscriptId() {
        when(segmentRepository.existsByTranscriptId(transcriptId)).thenReturn(true);

        transcriptService.handleTranscript(event(1));

        verify(sessionRepository, never()).findById(any());
        verify(segmentRepository, never()).save(any());
    }

    @Test
    void throwsWhenSessionNotYetCreated() {
        when(segmentRepository.existsByTranscriptId(transcriptId)).thenReturn(false);
        when(sessionRepository.findById(sessionId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> transcriptService.handleTranscript(event(1)))
                .isInstanceOf(UnknownSessionException.class)
                .hasMessageContaining(sessionId.toString());

        verify(segmentRepository, never()).save(any());
    }

    @Test
    void rejectsNullDataWithIllegalArgument() {
        MeetingTranscriptEvent bad = new MeetingTranscriptEvent(
                "meeting.transcript",
                new MeetingTranscriptEvent.MeetingRef(UUID.randomUUID(), sessionId),
                null);

        assertThatThrownBy(() -> transcriptService.handleTranscript(bad))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("data");

        verifyNoInteractions(segmentRepository);
    }

    @Test
    void rejectsNullMeetingWithIllegalArgument() {
        MeetingTranscriptEvent bad = new MeetingTranscriptEvent(
                "meeting.transcript", null,
                new MeetingTranscriptEvent.TranscriptData(
                        transcriptId, 1, null, "content", "0", "5", "en"));

        assertThatThrownBy(() -> transcriptService.handleTranscript(bad))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sessionId");
    }

    @Test
    void rejectsMissingRequiredDataFieldsWithIllegalArgument() {
        // Missing transcriptId
        MeetingTranscriptEvent bad = new MeetingTranscriptEvent(
                "meeting.transcript",
                new MeetingTranscriptEvent.MeetingRef(UUID.randomUUID(), sessionId),
                new MeetingTranscriptEvent.TranscriptData(
                        null, 1, null, "content", "0", "5", "en"));

        assertThatThrownBy(() -> transcriptService.handleTranscript(bad))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("transcriptId");

        verifyNoInteractions(segmentRepository);
    }

    @Test
    void defaultsLanguageToEnWhenMissing() {
        Session session = Session.builder().id(sessionId)
                .status(SessionStatus.LIVE).startedAt(Instant.now()).build();
        when(segmentRepository.existsByTranscriptId(transcriptId)).thenReturn(false);
        when(sessionRepository.findById(sessionId)).thenReturn(Optional.of(session));

        MeetingTranscriptEvent noLang = new MeetingTranscriptEvent(
                "meeting.transcript",
                new MeetingTranscriptEvent.MeetingRef(UUID.randomUUID(), sessionId),
                new MeetingTranscriptEvent.TranscriptData(
                        transcriptId, 1,
                        new MeetingTranscriptEvent.Speaker(UUID.randomUUID(), "Bob"),
                        "No language field", "0", "5", null));

        transcriptService.handleTranscript(noLang);

        ArgumentCaptor<TranscriptSegment> captor = ArgumentCaptor.forClass(TranscriptSegment.class);
        verify(segmentRepository).save(captor.capture());
        assertThat(captor.getValue().getLanguage()).isEqualTo("en");
    }
}
