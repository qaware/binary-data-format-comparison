package de.qaware.parquet.avro;

import de.qaware.api.TestReader;
import de.qaware.parquet.HadoopFile;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificData;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public class AvroParquetTestReader<T> implements TestReader {
    private final Path inputFile;
    private final Class<T> type;

    @Override
    public List<T> read() throws IOException {
        return readSpecificRecords();
    }

    @Override
    public Path getInputFile() {
        return inputFile;
    }

    /**
     * Read records into the <code>type</code> objects.
     *
     * @return records
     * @throws IOException on IO error
     */
    public List<T> readSpecificRecords() throws IOException {
        InputFile file = HadoopFile.hadoopInputFile(inputFile);
        ParquetReader<T> reader = AvroParquetReader.<T>builder(file)
                .withDataModel(SpecificData.getForClass(type))
                .build();
        return readAll(reader);
    }


    private <V> List<V> readAll(ParquetReader<V> reader) throws IOException {
        List<V> entries = new ArrayList<>();
        V entry;
        while ((entry = reader.read()) != null) {
            entries.add(entry);
        }
        return entries;
    }
}
