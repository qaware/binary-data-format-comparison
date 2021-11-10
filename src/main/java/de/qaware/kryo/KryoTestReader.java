package de.qaware.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import de.qaware.api.TestReader;
import de.qaware.compression.Compression;
import lombok.RequiredArgsConstructor;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public class KryoTestReader implements TestReader {
    private final Path inputFile;

    @Override
    public List<?> read() throws IOException {
        Kryo kryo = new Kryo();
        kryo.register(SampleDataKryo.class);

        try (FileInputStream fileInputStream = new FileInputStream(inputFile.toFile());
             InputStream compressionStream = encodeInputStream(fileInputStream);
             Input input = new Input(compressionStream)) {
            List<Object> records = new ArrayList<>();

            while (input.available() > 0) {
                SampleDataKryo entity = kryo.readObject(input, SampleDataKryo.class);
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
