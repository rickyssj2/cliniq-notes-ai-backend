package ai.soulside.storage;

import ai.soulside.common.config.AppProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class LocalFileStorageServiceTest {

    @TempDir
    Path tempDir;

    private LocalFileStorageService newService() {
        AppProperties props = new AppProperties();
        props.getStorage().setBasePath(tempDir.toString());
        return new LocalFileStorageService(props);
    }

    @Test
    void storesAndRetrievesContent() {
        LocalFileStorageService service = newService();
        String sessionId = UUID.randomUUID().toString();

        String uri = service.store(sessionId, "line one\nline two\n");

        assertThat(uri).contains(sessionId + ".txt");
        assertThat(Files.exists(tempDir.resolve(sessionId + ".txt"))).isTrue();
        assertThat(service.retrieve(sessionId)).contains("line one\nline two\n");
    }

    @Test
    void retrieveReturnsEmptyWhenAbsent() {
        LocalFileStorageService service = newService();
        assertThat(service.retrieve(UUID.randomUUID().toString())).isEqualTo(Optional.empty());
    }

    @Test
    void createsBaseDirectoryIfMissing() {
        AppProperties props = new AppProperties();
        Path nested = tempDir.resolve("nested/transcripts");
        props.getStorage().setBasePath(nested.toString());
        LocalFileStorageService service = new LocalFileStorageService(props);

        String sessionId = UUID.randomUUID().toString();
        service.store(sessionId, "content");

        assertThat(Files.exists(nested.resolve(sessionId + ".txt"))).isTrue();
    }
}
