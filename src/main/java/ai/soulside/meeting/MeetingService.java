package ai.soulside.meeting;

import ai.soulside.meeting.event.MeetingEndedEvent;
import ai.soulside.meeting.event.MeetingStartedEvent;
import ai.soulside.meeting.model.Meeting;
import ai.soulside.meeting.model.Session;
import ai.soulside.meeting.model.SessionStatus;
import ai.soulside.meeting.repository.MeetingRepository;
import ai.soulside.meeting.repository.SessionRepository;
import ai.soulside.transcript.ReconstructTaskProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Applies meeting lifecycle events to the database.
 *
 * <p>All handlers are idempotent so that duplicate Kafka deliveries (at-least-once semantics)
 * do not corrupt state or throw.
 */
@Service
public class MeetingService {

    private static final Logger log = LoggerFactory.getLogger(MeetingService.class);

    private final MeetingRepository meetingRepository;
    private final SessionRepository sessionRepository;
    private final ReconstructTaskProducer reconstructTaskProducer;

    public MeetingService(MeetingRepository meetingRepository,
                          SessionRepository sessionRepository,
                          ReconstructTaskProducer reconstructTaskProducer) {
        this.meetingRepository = meetingRepository;
        this.sessionRepository = sessionRepository;
        this.reconstructTaskProducer = reconstructTaskProducer;
    }

    /**
     * Handle {@code meeting.started}: upsert the Meeting and create a LIVE Session.
     * Idempotent — a duplicate delivery finds the session already present and does nothing.
     */
    @Transactional
    public void handleMeetingStarted(MeetingStartedEvent event) {
        MeetingStartedEvent.MeetingPayload payload = event.meeting();
        UUID meetingId = payload.id();
        UUID sessionId = payload.sessionId();

        Meeting meeting = upsertMeeting(payload);

        if (sessionRepository.existsById(sessionId)) {
            log.info("Session already exists, skipping creation. sessionId={}", sessionId);
            return;
        }

        Session session = Session.builder()
                .id(sessionId)
                .meeting(meeting)
                .status(SessionStatus.LIVE)
                .startedAt(payload.startedAt() != null ? payload.startedAt() : Instant.now())
                .build();
        sessionRepository.save(session);

        log.info("Created LIVE session. meetingId={} sessionId={}", meetingId, sessionId);
    }

    /**
     * Handle {@code meeting.ended}: mark the session ENDED and request transcript reconstruction.
     * Idempotent — an already-ENDED session is left unchanged. An unknown session raises
     * {@link UnknownSessionException} so the event can be retried and eventually dead-lettered.
     */
    @Transactional
    public void handleMeetingEnded(MeetingEndedEvent event) {
        MeetingEndedEvent.MeetingPayload payload = event.meeting();
        UUID sessionId = payload.sessionId();

        Session session = sessionRepository.findById(sessionId)
                .orElseThrow(() -> new UnknownSessionException(
                        "Cannot end unknown session: " + sessionId));

        if (session.getStatus() == SessionStatus.ENDED) {
            log.info("Session already ENDED, skipping. sessionId={}", sessionId);
            return;
        }

        session.setStatus(SessionStatus.ENDED);
        session.setEndedAt(payload.endedAt() != null ? payload.endedAt() : Instant.now());
        sessionRepository.save(session);

        log.info("Ended session. sessionId={} reason={}", sessionId, event.reason());

        // Signal downstream assembly of the full transcript.
        reconstructTaskProducer.requestReconstruction(sessionId.toString());
    }

    /**
     * Create the Meeting if absent, otherwise refresh its mutable metadata.
     */
    private Meeting upsertMeeting(MeetingStartedEvent.MeetingPayload payload) {
        return meetingRepository.findById(payload.id())
                .map(existing -> {
                    existing.setTitle(payload.title());
                    existing.setRoomName(payload.roomName());
                    applyOrganizer(existing, payload.organizedBy());
                    return meetingRepository.save(existing);
                })
                .orElseGet(() -> {
                    Meeting meeting = Meeting.builder()
                            .id(payload.id())
                            .title(payload.title())
                            .roomName(payload.roomName())
                            .createdAt(payload.createdAt() != null ? payload.createdAt() : Instant.now())
                            .build();
                    applyOrganizer(meeting, payload.organizedBy());
                    return meetingRepository.save(meeting);
                });
    }

    private void applyOrganizer(Meeting meeting, MeetingStartedEvent.Organizer organizer) {
        if (organizer != null) {
            meeting.setOrganizedById(organizer.id());
            meeting.setOrganizedByName(organizer.name());
        }
    }
}
