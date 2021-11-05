package de.qaware.json;

import de.qaware.api.TestReader;
import de.qaware.compression.Compression;
import lombok.RequiredArgsConstructor;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public class JsonTestReader implements TestReader {
    private final Jsonb jsonb = JsonbBuilder.create();
    private final Path inputFile;

    @Override
    public List<?> read() throws IOException {
        try (InputStream fileInStream = new FileInputStream(inputFile.toFile());
             BufferedReader reader = new BufferedReader(new InputStreamReader(encodeInputStream(fileInStream)))) {
            List<Object> records = new ArrayList<>();
            while (reader.ready()) {
                String line = reader.readLine();
                SampleDataJson entity = jsonb.fromJson(line, SampleDataJson.class);
                records.add(entity);
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
