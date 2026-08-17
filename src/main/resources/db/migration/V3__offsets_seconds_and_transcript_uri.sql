-- ============================================================================
-- V3: Store transcript offsets as whole seconds; track assembled transcript URI
-- ============================================================================

-- Offsets are relative-to-session-start seconds (per the assignment). Store as INTEGER.
-- Existing rows: cast where possible. Fresh dev DBs have no data.
ALTER TABLE transcript_segments
    ALTER COLUMN start_offset TYPE INTEGER USING NULLIF(start_offset, '')::INTEGER,
    ALTER COLUMN end_offset   TYPE INTEGER USING NULLIF(end_offset, '')::INTEGER;

-- Location of the assembled transcript once a session ends and is reconstructed.
ALTER TABLE sessions
    ADD COLUMN transcript_uri VARCHAR(1024);
