package ai.soulside.storage;

import ai.soulside.common.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Stores assembled transcripts as UTF-8 text files under a configured base directory,
 * one file per session: {@code {basePath}/{sessionId}.txt}.
 */
@Service
public class LocalFileStorageService implements StorageService {

    private static final Logger log = LoggerFactory.getLogger(LocalFileStorageService.class);

    private final Path basePath;

    public LocalFileStorageService(AppProperties properties) {
        this.basePath = Path.of(properties.getStorage().getBasePath());
    }

    @Override
    public String store(String sessionId, String content) {
        try {
            Files.createDirectories(basePath);
            Path target = filePath(sessionId);
            Files.writeString(target, content, StandardCharsets.UTF_8);
            String uri = target.toUri().toString();
            log.info("Stored transcript. sessionId={} uri={}", sessionId, uri);
            return uri;
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to store transcript for session " + sessionId, e);
        }
    }

    @Override
    public Optional<String> retrieve(String sessionId) {
        Path target = filePath(sessionId);
        if (!Files.exists(target)) {
            return Optional.empty();
        }
        try {
            return Optional.of(Files.readString(target, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read transcript for session " + sessionId, e);
        }
    }

    private Path filePath(String sessionId) {
        return basePath.resolve(sessionId + ".txt");
    }
}
