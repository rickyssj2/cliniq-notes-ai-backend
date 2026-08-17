package ai.soulside.meeting;

import ai.soulside.meeting.event.MeetingEndedEvent;
import ai.soulside.meeting.event.MeetingStartedEvent;
import ai.soulside.meeting.model.Meeting;
import ai.soulside.meeting.model.Session;
import ai.soulside.meeting.model.SessionStatus;
import ai.soulside.meeting.repository.MeetingRepository;
import ai.soulside.meeting.repository.SessionRepository;
import ai.soulside.transcript.ReconstructTaskProducer;
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
class MeetingServiceTest {

    @Mock
    private MeetingRepository meetingRepository;
    @Mock
    private SessionRepository sessionRepository;
    @Mock
    private ReconstructTaskProducer reconstructTaskProducer;

    @InjectMocks
    private MeetingService meetingService;

    private UUID meetingId;
    private UUID sessionId;

    @BeforeEach
    void setUp() {
        meetingId = UUID.randomUUID();
        sessionId = UUID.randomUUID();
    }

    private MeetingStartedEvent startedEvent() {
        return new MeetingStartedEvent("meeting.started",
                new MeetingStartedEvent.MeetingPayload(
                        meetingId, sessionId, "Q4 Sync", "room-1", "LIVE",
                        Instant.parse("2024-12-13T06:57:09.736Z"),
                        Instant.parse("2024-12-13T06:57:09.736Z"),
                        new MeetingStartedEvent.Organizer(UUID.randomUUID(), "Alice")));
    }

    private MeetingEndedEvent endedEvent() {
        return new MeetingEndedEvent("meeting.ended",
                new MeetingEndedEvent.MeetingPayload(
                        meetingId, sessionId, "Q4 Sync", "LIVE",
                        Instant.parse("2024-12-13T06:57:09.736Z"),
                        Instant.parse("2024-12-13T06:57:09.736Z"),
                        Instant.parse("2024-12-13T07:04:37.052Z"),
                        new MeetingEndedEvent.Organizer(UUID.randomUUID(), "Alice")),
                "HOST_ENDED_MEETING");
    }

    @Test
    void meetingStartedCreatesMeetingAndSession() {
        when(meetingRepository.findById(meetingId)).thenReturn(Optional.empty());
        when(meetingRepository.save(any(Meeting.class))).thenAnswer(inv -> inv.getArgument(0));
        when(sessionRepository.existsById(sessionId)).thenReturn(false);

        meetingService.handleMeetingStarted(startedEvent());

        verify(meetingRepository).save(any(Meeting.class));
        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(sessionRepository).save(sessionCaptor.capture());
        assertThat(sessionCaptor.getValue().getStatus()).isEqualTo(SessionStatus.LIVE);
        assertThat(sessionCaptor.getValue().getId()).isEqualTo(sessionId);
    }

    @Test
    void duplicateMeetingStartedDoesNotCreateSecondSession() {
        Meeting existing = Meeting.builder().id(meetingId).title("Q4 Sync")
                .createdAt(Instant.now()).build();
        when(meetingRepository.findById(meetingId)).thenReturn(Optional.of(existing));
        when(meetingRepository.save(any(Meeting.class))).thenAnswer(inv -> inv.getArgument(0));
        when(sessionRepository.existsById(sessionId)).thenReturn(true);

        meetingService.handleMeetingStarted(startedEvent());

        verify(sessionRepository, never()).save(any(Session.class));
    }

    @Test
    void meetingEndedMarksSessionEndedAndRequestsReconstruction() {
        Meeting meeting = Meeting.builder().id(meetingId).title("Q4 Sync")
                .createdAt(Instant.now()).build();
        Session live = Session.builder().id(sessionId).meeting(meeting)
                .status(SessionStatus.LIVE).startedAt(Instant.now()).build();
        when(sessionRepository.findById(sessionId)).thenReturn(Optional.of(live));

        meetingService.handleMeetingEnded(endedEvent());

        assertThat(live.getStatus()).isEqualTo(SessionStatus.ENDED);
        assertThat(live.getEndedAt()).isEqualTo(Instant.parse("2024-12-13T07:04:37.052Z"));
        verify(sessionRepository).save(live);
        verify(reconstructTaskProducer).requestReconstruction(sessionId.toString());
    }

    @Test
    void meetingEndedForAlreadyEndedSessionIsNoOp() {
        Session ended = Session.builder().id(sessionId)
                .status(SessionStatus.ENDED).startedAt(Instant.now())
                .endedAt(Instant.now()).build();
        when(sessionRepository.findById(sessionId)).thenReturn(Optional.of(ended));

        meetingService.handleMeetingEnded(endedEvent());

        verify(sessionRepository, never()).save(any(Session.class));
        verify(reconstructTaskProducer, never()).requestReconstruction(any());
    }

    @Test
    void meetingStartedWithNullMeetingThrowsIllegalArgument() {
        MeetingStartedEvent bad = new MeetingStartedEvent("meeting.started", null);

        assertThatThrownBy(() -> meetingService.handleMeetingStarted(bad))
                .isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(meetingRepository, sessionRepository);
    }

    @Test
    void meetingEndedWithNullSessionIdThrowsIllegalArgument() {
        MeetingEndedEvent bad = new MeetingEndedEvent("meeting.ended",
                new MeetingEndedEvent.MeetingPayload(
                        meetingId, null, "T", "LIVE", null, null, null, null),
                "HOST_ENDED_MEETING");

        assertThatThrownBy(() -> meetingService.handleMeetingEnded(bad))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sessionId");

        verifyNoInteractions(sessionRepository, reconstructTaskProducer);
    }

    @Test
    void meetingEndedForUnknownSessionThrows() {
        when(sessionRepository.findById(sessionId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> meetingService.handleMeetingEnded(endedEvent()))
                .isInstanceOf(UnknownSessionException.class)
                .hasMessageContaining(sessionId.toString());

        verify(reconstructTaskProducer, never()).requestReconstruction(any());
    }
}
