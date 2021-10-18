package de.qaware.sqlite;

import com.google.common.collect.Lists;
import de.qaware.api.TestWriter;
import de.qaware.compression.Compression;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.PreparedBatch;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class SqLiteTestWriter implements TestWriter {
    private final Path destFile;
    private final Compression compression;

    @Override
    public void write(List<GenericRecord> records) throws IOException {
        if (Files.deleteIfExists(destFile)) {
            log.debug("Deleted existing destination file {}", destFile);
        }

        Jdbi jdbi = Jdbi.create("jdbc:sqlite:" + destFile);

        jdbi.useHandle(handle -> handle.execute("CREATE TABLE sample_data (" +
                "int1 int, " +
                "int2 int, " +
                "int3 int, " +
                "int4 int, " +
                "int5 int, " +
                "string1 string, " +
                "string2 string, " +
                "string3 string, " +
                "string4 string, " +
                "string5 string " +
                ")"
        ));

        jdbi.useTransaction(handle -> {
            PreparedBatch preparedBatch = handle.prepareBatch("INSERT INTO sample_data(" +
                    "int1, int2, int3, int4, int5, string1, string2, string3, string4, string5" +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            List<List<GenericRecord>> partitions = Lists.partition(records, 10_000);
            for (var partition : partitions) {
                for (var entry : partition) {
                    preparedBatch.add(
                            entry.get("int1"),
                            entry.get("int2"),
                            entry.get("int3"),
                            entry.get("int4"),
                            entry.get("int5"),
                            entry.get("string1"),
                            entry.get("string2"),
                            entry.get("string3"),
                            entry.get("string4"),
                            entry.get("string5")
                    );
                }
                preparedBatch.execute();
            }
        });

        if (compression != Compression.NONE) {
            try (FileInputStream inputStream = new FileInputStream(destFile.toFile());
                 OutputStream outputStream = compression.outputStream(
                         new FileOutputStream(destFile + compression.getEnding()))) {
                int data;
                while ((data = inputStream.read()) != -1) {
                    outputStream.write(data);
                }
            }
        }
    }

    @Override
    public Path getOutputFile() {
        return Path.of(destFile + compression.getEnding());
    }
}
