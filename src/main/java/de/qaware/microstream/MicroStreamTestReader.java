package de.qaware.microstream;

import de.qaware.api.TestReader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import one.microstream.storage.embedded.types.EmbeddedStorage;
import one.microstream.storage.embedded.types.EmbeddedStorageManager;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class MicroStreamTestReader implements TestReader {
    private final Path inputFile;

    @SuppressWarnings("unchecked")
    @Override
    public List<?> read() throws IOException {
        EmbeddedStorageManager storageManager = EmbeddedStorage.start(inputFile);

        List<SampleDataMicroStream> entries = (List<SampleDataMicroStream>) storageManager.root();
        storageManager.storeRoot();

        storageManager.shutdown();
        return entries;
    }

    @Override
    public Path getInputFile() {
        return inputFile;
    }
}
