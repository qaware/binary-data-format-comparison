package de.qaware.avro;

import de.qaware.api.TestWriter;
import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

@RequiredArgsConstructor
public class AvroTestWriter implements TestWriter {
    private final Path schemaFile;
    private final Path destFile;
    private final CodecFactory codecFactory;

    @Override
    public void write(List<GenericRecord> records) throws IOException {
        Schema schema = new Schema.Parser().parse(schemaFile.toFile());
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.setCodec(codecFactory);
            dataFileWriter.create(schema, destFile.toFile());
            for (var entry : records) {
                dataFileWriter.append(entry);
            }
        }
    }

    @Override
    public Path getOutputFile() {
        return destFile;
    }
}
