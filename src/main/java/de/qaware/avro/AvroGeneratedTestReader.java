package de.qaware.avro;

import de.qaware.api.TestReader;
import lombok.RequiredArgsConstructor;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public class AvroGeneratedTestReader<T> implements TestReader {
    private final Path inputFile;
    private final Class<T> type;

    @Override
    public List<T> read() throws IOException {
        DatumReader<T> datumReader = new SpecificDatumReader<>(type);
        try (DataFileReader<T> dataFileReader = new DataFileReader<>(inputFile.toFile(), datumReader)) {
            List<T> records = new ArrayList<>();
            while (dataFileReader.hasNext()) {
                records.add(dataFileReader.next());
            }
            return records;
        }
    }

    @Override
    public Path getInputFile() {
        return inputFile;
    }
}
