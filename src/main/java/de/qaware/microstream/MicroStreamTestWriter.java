package de.qaware.microstream;

import de.qaware.api.TestWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import one.microstream.storage.embedded.types.EmbeddedStorage;
import one.microstream.storage.embedded.types.EmbeddedStorageManager;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
public class MicroStreamTestWriter implements TestWriter {
    private final Path destFile;

    @Override
    public void write(List<GenericRecord> records) throws IOException {
        if (destFile.toFile().exists()) {
            try (Stream<Path> stream = Files.walk(destFile)) {
                boolean allDeleted = stream.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .map(File::delete)
                        .reduce((a, b) -> a && b)
                        .orElse(false);
                if (!allDeleted) {
                    log.warn("Could not delete all existing files in {}", destFile);
                }
            }
        }

        EmbeddedStorageManager storageManager = EmbeddedStorage.start(destFile);

        var microStreamRoot = records.stream()
                .map(this::map)
                .collect(Collectors.toUnmodifiableList());
        storageManager.setRoot(microStreamRoot);
        storageManager.storeRoot();
        storageManager.issueFullGarbageCollection();
        storageManager.issueFullCacheCheck();

        storageManager.shutdown();
    }

    private SampleDataMicroStream map(GenericRecord genericRecord) {
        return SampleDataMicroStream.builder()
                .int1((int) genericRecord.get("int1"))
                .int2((int) genericRecord.get("int2"))
                .int3((int) genericRecord.get("int3"))
                .int4((int) genericRecord.get("int4"))
                .int5((int) genericRecord.get("int5"))
                .string1(String.valueOf(genericRecord.get("string1")))
                .string2(String.valueOf(genericRecord.get("string2")))
                .string3(String.valueOf(genericRecord.get("string3")))
                .string4(String.valueOf(genericRecord.get("string4")))
                .string5(String.valueOf(genericRecord.get("string5")))
                .build();
    }

    @Override
    public Path getOutputFile() {
        return destFile;
    }
}
