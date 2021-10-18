package de.qaware.proto;

import de.qaware.api.TestWriter;
import de.qaware.compression.Compression;
import de.qaware.data.SampleData;
import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;

@RequiredArgsConstructor
public class ProtoTestWriter implements TestWriter {
    private final Path destFile;
    private final Compression compression;


    @Override
    public void write(List<GenericRecord> records) throws IOException {
        try (DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(destFile.toFile(), false));
             OutputStream outputStream = compression.outputStream(dataOutputStream)) {
            for (var genericRecord : records) {
                var pb = map(genericRecord);
                pb.writeDelimitedTo(outputStream);
            }
        }
    }

    private SampleData.SampleDataPb map(GenericRecord genericRecord) {
        return SampleData.SampleDataPb.newBuilder()
                .setInt1((int) genericRecord.get("int1"))
                .setInt2((int) genericRecord.get("int2"))
                .setInt3((int) genericRecord.get("int3"))
                .setInt4((int) genericRecord.get("int4"))
                .setInt5((int) genericRecord.get("int5"))
                .setString1(String.valueOf(genericRecord.get("string1")))
                .setString2(String.valueOf(genericRecord.get("string2")))
                .setString3(String.valueOf(genericRecord.get("string3")))
                .setString4(String.valueOf(genericRecord.get("string4")))
                .setString5(String.valueOf(genericRecord.get("string5")))
                .build();
    }

    @Override
    public Path getOutputFile() {
        return destFile;
    }
}
