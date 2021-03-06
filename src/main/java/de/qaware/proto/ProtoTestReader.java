package de.qaware.proto;

import de.qaware.api.TestReader;
import de.qaware.compression.Compression;
import de.qaware.data.SampleData;
import lombok.RequiredArgsConstructor;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public class ProtoTestReader implements TestReader {
    private final Path inputFile;

    @Override
    public List<Object> read() throws IOException {
        try (InputStream fileInStream = new BufferedInputStream(new FileInputStream(inputFile.toFile()));
             InputStream inputStream = encodeInputStream(fileInStream)) {
            List<Object> records = new ArrayList<>();
            SampleData.SampleDataPb pb;
            while ((pb = SampleData.SampleDataPb.parseDelimitedFrom(inputStream)) != null) {
                records.add(pb);
            }
            return records;
        }
    }

    @Override
    public Path getInputFile() {
        return inputFile;
    }

    private InputStream encodeInputStream(InputStream inputStream) throws IOException {
        return Compression.fromFile(inputFile).inputStream(inputStream);
    }
}
