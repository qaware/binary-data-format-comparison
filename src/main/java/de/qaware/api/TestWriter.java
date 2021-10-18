package de.qaware.api;

import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface TestWriter {
    void write(List<GenericRecord> records) throws IOException;

    Path getOutputFile();
}
