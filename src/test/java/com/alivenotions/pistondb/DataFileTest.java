package com.alivenotions.pistondb;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DataFileTest {

    private DataFile dataFile;
    private static final String TEST_PATH = "src/test/data";
    private static final File dataDir = new File(TEST_PATH);

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Before
    public void setup() throws FileNotFoundException {
        dataDir.mkdirs();

        dataFile = DataFile.init(dataDir);
    }

    @After
    public void teardown() throws IOException, InterruptedException {
        if (dataFile != null) {
            dataFile.close();
        }
        removeDataDir();
    }

    private void removeDataDir() throws IOException, InterruptedException {
        Process p = Runtime.getRuntime().exec(new String[] {"rm", "-Rf", TEST_PATH});
        p.waitFor();
    }

    private static String getTestFileName() throws FileNotFoundException {
        File[] testFiles = dataDir.listFiles();
        if (Objects.requireNonNull(testFiles).length > 0) {
            return testFiles[0].getPath();
        }
        throw new FileNotFoundException("No test file exists");
    }

    public byte[] readFile(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        return Files.readAllBytes(path);
    }

    @Test
    public void testOffset() throws IOException {
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");

        long offset = dataFile.write(key, value, 1696808073);
        assertEquals(offset, 24);
    }

    @Test
    public void shouldStoreTheValueCorrectlyForFreshFile() throws IOException {
        ByteString key = ByteString.copyFromUtf8("hello");
        ByteString value = ByteString.copyFromUtf8("okay");
        dataFile.write(key, value, 1696808073);
        byte[] data = readFile(getTestFileName());

        assertEquals(16 + 5 + 4, data.length);
        assertEquals("hello", new String(data, 16, 5, StandardCharsets.UTF_8));
        assertEquals("okay", new String(data, 21, 4, StandardCharsets.UTF_8));
    }
}
