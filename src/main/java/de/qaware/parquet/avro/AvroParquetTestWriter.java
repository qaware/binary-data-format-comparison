package de.qaware.parquet.avro;

import de.qaware.api.TestWriter;
import de.qaware.parquet.HadoopFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@RequiredArgsConstructor
@Slf4j
public class AvroParquetTestWriter implements TestWriter {
    private final Path destFile;
    private final Path schemaFile;
    private final CompressionCodecName compressionCodecName;


    @Override
    public void write(List<GenericRecord> records) throws IOException {
        if (Files.deleteIfExists(destFile)) {
            log.debug("Existing destination file deleted: {}", destFile);
        }
        Schema schema = new Schema.Parser().parse(schemaFile.toFile());
        ParquetWriter<GenericRecord> writer =
                AvroParquetWriter.<GenericRecord>builder(HadoopFile.hadoopOutputFile(destFile))
                        .withCompressionCodec(compressionCodecName)
                        .withSchema(schema)
                        .build();
        for (var entry : records) {
            writer.write(entry);
        }
        writer.close();
    }

    @Override
    public Path getOutputFile() {
        return destFile;
    }
}
