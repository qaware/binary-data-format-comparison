package de.qaware.avro;

import de.qaware.api.TestReader;
import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public class AvroTestReader implements TestReader {
    private final Path inputFile;
    private final Path schemaFile;

    @Override
    public List<GenericRecord> read() throws IOException {
        Schema schema = new Schema.Parser().parse(schemaFile.toFile());
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(inputFile.toFile(), datumReader)) {
            List<GenericRecord> records = new ArrayList<>();
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
