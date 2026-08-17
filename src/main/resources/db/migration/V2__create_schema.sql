-- ============================================================================
-- V2: Core domain schema — meetings, sessions, transcript_segments
-- ============================================================================

CREATE TABLE meetings (
    id              UUID            PRIMARY KEY,
    title           VARCHAR(500)    NOT NULL,
    room_name       VARCHAR(255),
    organized_by_id UUID,
    organized_by_name VARCHAR(255),
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE TABLE sessions (
    id              UUID            PRIMARY KEY,
    meeting_id      UUID            NOT NULL REFERENCES meetings(id),
    status          VARCHAR(10)     NOT NULL DEFAULT 'LIVE',
    started_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    ended_at        TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),

    CONSTRAINT chk_session_status CHECK (status IN ('LIVE', 'ENDED'))
);

CREATE INDEX idx_sessions_meeting_id ON sessions(meeting_id);
CREATE INDEX idx_sessions_status ON sessions(status);

CREATE TABLE transcript_segments (
    id                  BIGSERIAL       PRIMARY KEY,
    transcript_id       UUID            NOT NULL,
    session_id          UUID            NOT NULL REFERENCES sessions(id),
    sequence_number     INTEGER         NOT NULL,
    speaker_id          UUID,
    speaker_name        VARCHAR(255),
    content             TEXT            NOT NULL,
    start_offset        VARCHAR(50)     NOT NULL,
    end_offset          VARCHAR(50)     NOT NULL,
    language            VARCHAR(10)     DEFAULT 'en',
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),

    -- Deduplication: each transcript chunk is uniquely identified by transcript_id
    CONSTRAINT uq_transcript_id UNIQUE (transcript_id)
);

CREATE INDEX idx_transcript_segments_session_order
    ON transcript_segments(session_id, sequence_number);
