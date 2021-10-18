package de.qaware.api;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface TestReader {
    List<?> read() throws IOException;

    Path getInputFile();
}
