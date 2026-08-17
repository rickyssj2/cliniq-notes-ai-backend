package ai.soulside.meeting.repository;

import ai.soulside.meeting.model.Session;
import ai.soulside.meeting.model.SessionStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface SessionRepository extends JpaRepository<Session, UUID> {

    /**
     * Find sessions for a meeting with a given status.
     */
    List<Session> findByMeetingIdAndStatus(UUID meetingId, SessionStatus status);

    /**
     * Find a specific session that belongs to a given meeting.
     */
    Optional<Session> findByIdAndMeetingId(UUID sessionId, UUID meetingId);
}
