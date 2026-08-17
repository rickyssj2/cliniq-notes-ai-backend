package ai.soulside.transcript;

import ai.soulside.meeting.UnknownSessionException;
import ai.soulside.meeting.model.Session;
import ai.soulside.meeting.model.SessionStatus;
import ai.soulside.meeting.repository.SessionRepository;
import ai.soulside.storage.StorageService;
import ai.soulside.transcript.model.TranscriptSegment;
import ai.soulside.transcript.repository.TranscriptSegmentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TranscriptReconstructionServiceTest {

    @Mock
    private SessionRepository sessionRepository;
    @Mock
    private TranscriptSegmentRepository segmentRepository;
    @Mock
    private StorageService storageService;

    private TranscriptReconstructionService service;

    private UUID sessionId;

    @BeforeEach
    void setUp() {
        sessionId = UUID.randomUUID();
        // Real registry so counter(...).increment() works without stubbing Micrometer internals.
        service = new TranscriptReconstructionService(
                sessionRepository, segmentRepository, storageService,
                new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
    }

    private TranscriptSegment segment(int seq, String speaker, String content, int start, int end) {
        return TranscriptSegment.builder()
                .transcriptId(UUID.randomUUID())
                .sequenceNumber(seq)
                .speakerName(speaker)
                .content(content)
                .startOffset(start)
                .endOffset(end)
                .language("en")
                .build();
    }

    @Test
    void reconstructsOrderedTranscriptAndPersistsUri() {
        Session session = Session.builder().id(sessionId)
                .status(SessionStatus.ENDED).startedAt(Instant.now()).build();
        when(sessionRepository.findById(sessionId)).thenReturn(Optional.of(session));
        when(segmentRepository.findBySessionIdOrderBySequenceNumberAsc(sessionId))
                .thenReturn(List.of(
                        segment(1, "Alice", "Hello", 0, 5),
                        segment(2, "Bob", "Hi there", 5, 9)));
        when(storageService.store(eq(sessionId.toString()), anyString()))
                .thenReturn("file:///transcripts/" + sessionId + ".txt");

        service.reconstruct(sessionId);

        ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
        verify(storageService).store(eq(sessionId.toString()), contentCaptor.capture());
        String content = contentCaptor.getValue();
        assertThat(content).isEqualTo(
                "[0-5s] Alice: Hello\n[5-9s] Bob: Hi there\n");

        assertThat(session.getTranscriptUri()).isEqualTo("file:///transcripts/" + sessionId + ".txt");
        verify(sessionRepository).save(session);
    }

    @Test
    void storesEmptyTranscriptWhenNoSegments() {
        Session session = Session.builder().id(sessionId)
                .status(SessionStatus.ENDED).startedAt(Instant.now()).build();
        when(sessionRepository.findById(sessionId)).thenReturn(Optional.of(session));
        when(segmentRepository.findBySessionIdOrderBySequenceNumberAsc(sessionId))
                .thenReturn(List.of());
        when(storageService.store(eq(sessionId.toString()), eq("")))
                .thenReturn("file:///empty.txt");

        service.reconstruct(sessionId);

        verify(storageService).store(sessionId.toString(), "");
        verify(sessionRepository).save(session);
    }

    @Test
    void throwsForUnknownSession() {
        when(sessionRepository.findById(sessionId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.reconstruct(sessionId))
                .isInstanceOf(UnknownSessionException.class);

        verifyNoInteractions(storageService);
    }

    @Test
    void formatsUnknownSpeakerGracefully() {
        List<TranscriptSegment> segments = List.of(segment(1, null, "Anonymous line", 0, 2));
        String formatted = service.format(segments);
        assertThat(formatted).isEqualTo("[0-2s] Unknown: Anonymous line\n");
    }
}
