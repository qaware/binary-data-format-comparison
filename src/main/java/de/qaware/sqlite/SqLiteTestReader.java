package de.qaware.sqlite;

import de.qaware.api.TestReader;
import de.qaware.data.SampleDataAvro;
import lombok.RequiredArgsConstructor;
import org.jdbi.v3.core.Jdbi;

import java.nio.file.Path;
import java.util.List;

@RequiredArgsConstructor
public class SqLiteTestReader implements TestReader {
    private final Path inputFile;

    @Override
    public List<SampleDataAvro> read() {
        Jdbi jdbi = Jdbi.create("jdbc:sqlite:" + inputFile);

        return jdbi.withHandle(handle ->
                handle.createQuery("select * from sample_data")
                        .map(new SampleDataRowMapper())
                        .list()
        );
    }

    @Override
    public Path getInputFile() {
        return inputFile;
    }
}
