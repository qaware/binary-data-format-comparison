package de.qaware.json;

import de.qaware.api.TestWriter;
import de.qaware.compression.Compression;
import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;

@RequiredArgsConstructor
public class JsonTestWriter implements TestWriter {
    private final Jsonb jsonb = JsonbBuilder.create();
    private final Path destFile;
    private final Compression compression;

    @Override
    public void write(List<GenericRecord> records) throws IOException {
        try (DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(destFile.toFile(), false));
             OutputStream outputStream = compression.outputStream(dataOutputStream)) {
            for (var genericRecord : records) {
                var entity = map(genericRecord);
                String json = jsonb.toJson(entity) + "\n";
                outputStream.write(json.getBytes());
            }
        }
    }

    private SampleDataJson map(GenericRecord genericRecord) {
        return SampleDataJson.builder()
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
