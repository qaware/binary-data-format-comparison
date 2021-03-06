package de.qaware;

import de.qaware.api.TestReader;
import de.qaware.api.TestWriter;
import de.qaware.avro.AvroGeneratedTestReader;
import de.qaware.avro.AvroTestReader;
import de.qaware.avro.AvroTestWriter;
import de.qaware.compression.Compression;
import de.qaware.data.SampleDataAvro;
import de.qaware.json.JsonTestReader;
import de.qaware.json.JsonTestWriter;
import de.qaware.kryo.KryoTestReader;
import de.qaware.kryo.KryoTestWriter;
import de.qaware.microstream.MicroStreamTestReader;
import de.qaware.microstream.MicroStreamTestWriter;
import de.qaware.parquet.avro.AvroParquetTestReader;
import de.qaware.parquet.avro.AvroParquetTestWriter;
import de.qaware.parquet.group.GroupTestReader;
import de.qaware.proto.ProtoTestReader;
import de.qaware.proto.ProtoTestWriter;
import de.qaware.sqlite.SqLiteTestReader;
import de.qaware.sqlite.SqLiteTestWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Test data (de-) serialization.
 */
@Slf4j
@SuppressWarnings("java:S1215") // call GC here intentionally to measure heap usage
public class TestApp {
    private static final int NUM_DATA_SAMPLES = 10_000_000;
    private static final double NUM_STRINGS = 10_000;
    private static final long SEED = 42;

    Random random = new Random(SEED);

    Path basePath = Path.of(".");
    Path dataTargetPath = basePath.resolve("build/data");

    String file = "sample_data";
    Class<?> dataType = SampleDataAvro.class;

    Path inputAvsc = basePath.resolve("src/main/avro/" + file + ".avsc");

    Path outputParquet = dataTargetPath.resolve(file + ".parquet");
    Path outputParquetGzip = dataTargetPath.resolve(file + ".gzip.parquet");
    Path outputAvro = dataTargetPath.resolve(file + ".avro");
    Path outputAvroGzip = dataTargetPath.resolve(file + ".gzip.avro");
    Path outputAvroBzip2 = dataTargetPath.resolve(file + ".bzip2.avro");
    Path outputProto = dataTargetPath.resolve(file + ".protobuf");
    Path outputProtoGzip = dataTargetPath.resolve(file + ".protobuf.gzip");
    Path outputProtoBzip2 = dataTargetPath.resolve(file + ".protobuf.bzip2");
    Path outputSqLite = dataTargetPath.resolve(file + ".sqlite");
    Path outputJson = dataTargetPath.resolve(file + ".json");
    Path outputJsonGzip = dataTargetPath.resolve(file + ".json.gzip");
    Path outputKryo = dataTargetPath.resolve(file + ".kryo");
    Path outputKryoGzip = dataTargetPath.resolve(file + ".kryo.gzip");
    Path outputMicroStream = dataTargetPath.resolve(file + ".microStream");

    /**
     * Entry point.
     *
     * @param args args
     * @throws IOException on IO error
     */
    public static void main(String[] args) throws IOException {
        new TestApp().run();
        // MicroStream keeps threads running even after shutting down the storageManager...
        // That hinders the JVM from existing automatically.
        System.exit(0);
    }

    private void run() throws IOException {

        if (dataTargetPath.toFile().mkdirs()) {
            log.info("Created data dir: {}", dataTargetPath);
        }

        List<GenericRecord> allRecords = generateRandomData();
        writeAll(allRecords);
        readAndMeasureAll();
    }

    private void readAndMeasureAll() throws IOException {
        Map<String, TestReader> readerMap = new LinkedHashMap<>();
        readerMap.put("group.parquet", new GroupTestReader(outputParquet));
        readerMap.put("group.parquet.gzip", new GroupTestReader(outputParquetGzip));
        readerMap.put("gen.parquet", new AvroParquetTestReader<>(outputParquet, dataType));
        readerMap.put("gen.parquet.gzip", new AvroParquetTestReader<>(outputParquetGzip, dataType));
        readerMap.put("avro", new AvroTestReader(outputAvro, inputAvsc));
        readerMap.put("avro.gzip", new AvroTestReader(outputAvroGzip, inputAvsc));
        readerMap.put("avro.bzip2", new AvroTestReader(outputAvroBzip2, inputAvsc));
        readerMap.put("gen.avro", new AvroGeneratedTestReader<>(outputAvro, dataType));
        readerMap.put("gen.avro.gzip", new AvroGeneratedTestReader<>(outputAvroGzip, dataType));
        readerMap.put("gen.avro.bzip2", new AvroGeneratedTestReader<>(outputAvroBzip2, dataType));
        readerMap.put("proto", new ProtoTestReader(outputProto));
        readerMap.put("proto.gzip", new ProtoTestReader(outputProtoGzip));
        readerMap.put("proto.bzip2", new ProtoTestReader(outputProtoBzip2));
        readerMap.put("sqlite", new SqLiteTestReader(outputSqLite));
        readerMap.put("json", new JsonTestReader(outputJson));
        readerMap.put("json.gzip", new JsonTestReader(outputJsonGzip));
        readerMap.put("kryo", new KryoTestReader(outputKryo));
        readerMap.put("kryo.gzip", new KryoTestReader(outputKryoGzip));
        readerMap.put("microStream", new MicroStreamTestReader(outputMicroStream));

        System.gc();
        long baseHeapSize = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        File reportCsv = dataTargetPath.resolve("read-report.csv").toFile();
        try (FileWriter fileWriter = new FileWriter(reportCsv)) {
            for (var entry : readerMap.entrySet()) {
                String name = entry.getKey();
                TestReader reader = entry.getValue();
                long fileSize = fileOrFolderSize(reader.getInputFile());
                long tStart = System.nanoTime();
                List<?> objects = reader.read();
                long count = objects.size();
                System.gc();
                long heapSizeAfter =
                        Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory() - baseHeapSize;
                long tEnd = System.nanoTime();
                double dt = (tEnd - tStart) / 1e9;
                String clazz = objects.get(0).getClass().getSimpleName();
                log.info(String.format("Read %d entries in %.3fs from %d bytes with %d heap (%s)",
                        count, dt, fileSize, heapSizeAfter, name));
                fileWriter.write(
                        String.format("%s,%s,%d,%d,%.3f,%d%n", name, clazz, count, fileSize, dt, heapSizeAfter));
            }
        }
    }

    private void writeAll(List<GenericRecord> allRecords) throws IOException {

        Map<String, TestWriter> writerMap = new LinkedHashMap<>();
        writerMap.put("parquet",
                new AvroParquetTestWriter(outputParquet, inputAvsc, CompressionCodecName.UNCOMPRESSED));
        writerMap.put("parquet.gzip",
                new AvroParquetTestWriter(outputParquetGzip, inputAvsc, CompressionCodecName.GZIP));
        writerMap.put("avro", new AvroTestWriter(inputAvsc, outputAvro, CodecFactory.nullCodec()));
        writerMap.put("avro.gzip", new AvroTestWriter(inputAvsc, outputAvroGzip, CodecFactory.deflateCodec(9)));
        writerMap.put("avro.bzip2", new AvroTestWriter(inputAvsc, outputAvroBzip2, CodecFactory.bzip2Codec()));
        writerMap.put("proto", new ProtoTestWriter(outputProto, Compression.NONE));
        writerMap.put("proto.gzip", new ProtoTestWriter(outputProtoGzip, Compression.GZIP));
        writerMap.put("proto.bzip2", new ProtoTestWriter(outputProtoBzip2, Compression.BZIP2));
        writerMap.put("sqlite", new SqLiteTestWriter(outputSqLite, Compression.NONE));
        writerMap.put("sqlite.gzip", new SqLiteTestWriter(outputSqLite, Compression.GZIP));
        writerMap.put("sqlite.bzip2", new SqLiteTestWriter(outputSqLite, Compression.BZIP2));
        writerMap.put("json", new JsonTestWriter(outputJson, Compression.NONE));
        writerMap.put("json.gzip", new JsonTestWriter(outputJsonGzip, Compression.GZIP));
        writerMap.put("kryo", new KryoTestWriter(outputKryo, Compression.NONE));
        writerMap.put("kryo.gzip", new KryoTestWriter(outputKryoGzip, Compression.GZIP));
        writerMap.put("microStream", new MicroStreamTestWriter(outputMicroStream));

        File reportCsv = dataTargetPath.resolve("write-report.csv").toFile();
        try (FileWriter fileWriter = new FileWriter(reportCsv)) {
            for (var entry : writerMap.entrySet()) {
                String name = entry.getKey();
                TestWriter writer = entry.getValue();
                long tStart = System.nanoTime();
                writer.write(allRecords);
                long tEnd = System.nanoTime();
                double dt = (tEnd - tStart) / 1e9;
                log.info(String.format("Wrote %d entries in %.3fs (%s)", allRecords.size(), dt, name));
                long fileSize = fileOrFolderSize(writer.getOutputFile());
                fileWriter.write(String.format("%s,%d,%d,%.3f%n", name, allRecords.size(), fileSize, dt));
            }
        }
    }

    private long fileOrFolderSize(Path path) {
        var f = path.toFile();
        if (f.isFile()) {
            return f.length();
        } else if (f.isDirectory()) {
            try (Stream<Path> stream = Files.list(path)) {
                return stream.mapToLong(this::fileOrFolderSize).sum();
            } catch (IOException e) {
                log.error("Could not determine file/folder size", e);
                return 0;
            }
        }
        throw new IllegalStateException("Not a file nor a directory?!");
    }

    private List<GenericRecord> generateRandomData() {
        log.info("Generating {} random data samples", NUM_DATA_SAMPLES);

        List<String> stringPool = new ArrayList<>();
        for (int i = 0; i < NUM_STRINGS; i++) {
            stringPool.add(randomString());
        }

        List<GenericRecord> allRecords = new ArrayList<>(NUM_DATA_SAMPLES);
        for (int i = 0; i < NUM_DATA_SAMPLES; i++) {
            var sample = SampleDataAvro.newBuilder()
                    .setInt1(random.nextInt())
                    .setInt2(random.nextInt())
                    .setInt3(random.nextInt())
                    .setInt4(random.nextInt())
                    .setInt5(random.nextInt())
                    .setString1(nextString(stringPool))
                    .setString2(nextString(stringPool))
                    .setString3(nextString(stringPool))
                    .setString4(nextString(stringPool))
                    .setString5(nextString(stringPool))
                    .build();
            allRecords.add(sample);
        }
        log.info("Generated {} random data samples", allRecords.size());
        return allRecords;
    }

    private String nextString(List<String> stringPool) {
        return stringPool.get(random.nextInt(stringPool.size()));
    }

    private String randomString() {
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = random.nextInt(20);

        return random.ints(leftLimit, rightLimit + 1)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }
}
