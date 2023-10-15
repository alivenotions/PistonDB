package com.alivenotions.pistondb;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PistonTest {
    private static final String TEST_PATH = "src/test/data";
    private static final File dataDir = new File(TEST_PATH);

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Before
    public void setup() {
        dataDir.mkdirs();
    }

    @After
    public void teardown() throws IOException, InterruptedException {
        removeDataDir();
    }

    private void removeDataDir() throws IOException, InterruptedException {
        Process p = Runtime.getRuntime().exec(new String[] {"rm", "-Rf", TEST_PATH});
        p.waitFor();
    }

    @Test
    public void testOpen() throws IOException {
        Piston db = Piston.open(new File(TEST_PATH));
        assertNotNull(db);
        db.close();
    }

    @Test
    public void shouldWriteAndReadCorrectValue() throws IOException {
        Piston db = Piston.open(new File(TEST_PATH));
        ByteString key = ByteString.copyFromUtf8("hello");
        ByteString value = ByteString.copyFromUtf8("world");
        db.put(key, value);

        ByteString res = db.get(key).orElse(ByteString.empty());
        assertEquals("world", res.toStringUtf8());
    }

    @Test
    public void shouldNotReturnGarbageForNonExistentKey() throws IOException {
        Piston db = Piston.open(new File(TEST_PATH));
        ByteString key = ByteString.copyFromUtf8("hello");
        ByteString res = db.get(key).orElse(null);
        assertNull(res);
    }
}
