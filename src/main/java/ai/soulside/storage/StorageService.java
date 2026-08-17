package ai.soulside.storage;

import java.util.Optional;

/**
 * Abstraction over where assembled transcripts are persisted.
 *
 * <p>The local implementation writes files under a configured directory; the interface exists so
 * a cloud object store (S3, GCS) could be swapped in without touching reconstruction logic.
 */
public interface StorageService {

    /**
     * Store the assembled transcript for a session.
     *
     * @param sessionId the session the transcript belongs to
     * @param content   the full, ordered transcript text
     * @return a URI identifying where the content was stored
     */
    String store(String sessionId, String content);

    /**
     * Retrieve a previously stored transcript.
     *
     * @param sessionId the session
     * @return the transcript content, or empty if none has been stored
     */
    Optional<String> retrieve(String sessionId);
}
