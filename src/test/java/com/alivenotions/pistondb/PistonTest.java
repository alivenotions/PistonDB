package com.alivenotions.pistondb;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PistonTest {
    private static final String TEST_PATH = "src/test/data";

    @Before
    public void setup() throws FileNotFoundException {
        File dataDir = new File(TEST_PATH);
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
    }

    //    @Test
    public void shouldStoreTheValueCorrectly() throws IOException {
        Piston db = Piston.open(new File(TEST_PATH));
        ByteString key = ByteString.copyFromUtf8("hello");
        ByteString value = ByteString.copyFromUtf8("okay");
        db.put(key, value);

        ByteString k2 = ByteString.copyFromUtf8("hello");
        ByteString res = db.get(k2);
        String str = res.toStringUtf8();

        assertEquals("okay", str);
    }

    //    @Test
    public void shouldNotReturnGarbageForNonExistentKey() throws IOException {
        Piston db = Piston.open(new File(TEST_PATH));
        ByteString key = ByteString.copyFromUtf8("hello");
        ByteString res = db.get(key);
        assertNull(res);
    }
}
