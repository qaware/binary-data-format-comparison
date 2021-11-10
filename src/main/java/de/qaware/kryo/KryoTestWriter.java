package de.qaware.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import de.qaware.api.TestWriter;
import de.qaware.compression.Compression;
import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;

@RequiredArgsConstructor
public class KryoTestWriter implements TestWriter {
    private final Path destFile;
    private final Compression compression;

    @Override
    public void write(List<GenericRecord> records) throws IOException {
        Kryo kryo = new Kryo();
        kryo.register(SampleDataKryo.class);


        try (FileOutputStream fileOutputStream = new FileOutputStream(destFile.toFile());
             OutputStream outputStream = compression.outputStream(fileOutputStream);
             Output output = new Output(outputStream)) {
            for (var genericRecord : records) {
                var entity = map(genericRecord);
                kryo.writeObject(output, entity);
            }
            output.flush();
        }
    }

    private SampleDataKryo map(GenericRecord genericRecord) {
        return SampleDataKryo.builder()
                .int1((int) genericRecord.get("int1"))
                .int2((int) genericRecord.get("int2"))
                .int3((int) genericRecord.get("int3"))
                .int4((int) genericRecord.get("int4"))
                .int5((int) genericRecord.get("int5"))
                .string1(String.valueOf(genericRecord.get("string1")))
                .string2(String.valueOf(genericRecord.get("string2")))
                .string3(String.valueOf(genericRecord.get("string3")))
                .string4(String.valueOf(genericRecord.get("string4")))
                .string5(String.valueOf(genericRecord.get("string5")))
                .build();
    }

    @Override
    public Path getOutputFile() {
        return destFile;
    }
}
