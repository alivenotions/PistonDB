package com.alivenotions.pistondb;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DataFileTest {

    private DataFile dataFile;
    private static final String TEST_PATH = "src/test/data";

    @Before
    public void setup() throws IOException {
        File dataDir = new File(TEST_PATH);
        dataDir.mkdirs();

        dataFile = DataFile.create(dataDir);
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

    @Test
    public void testCreateAndWrite() throws IOException {
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");

        DirEntry dirEntry = dataFile.write(key, value);

        assertNotNull(dirEntry);
        assertEquals(value.size(), dirEntry.valueSize());
    }

    @Test
    public void testOpenAndWrite() throws IOException {
        File file = new File(TEST_PATH + "/123456.data");
        file.createNewFile();
        DataFile dataFile = DataFile.open(file);

        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");

        DirEntry dirEntry = dataFile.write(key, value);

        assertNotNull(dirEntry);
        assertEquals(value.size(), dirEntry.valueSize());
    }
}
