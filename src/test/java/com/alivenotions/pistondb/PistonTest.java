package com.alivenotions.pistondb;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.junit.Test;

public class PistonTest {

    @Test
    public void testOpen() {
        Piston db = Piston.open();
        assertNotNull(db);
    }

    @Test
    public void shouldStoreTheValueCorrectly() throws UnsupportedEncodingException, IOException {
        Piston db = Piston.open();
        ByteString key = ByteString.copyFromUtf8("hello");
        ByteString value = ByteString.copyFromUtf8("okay");
        db.put(key, value);

        ByteString k2 = ByteString.copyFromUtf8("hello");
        ByteString res = db.get(k2);
        String str = res.toStringUtf8();

        assertEquals("okay", str);
    }

    @Test
    public void shouldNotReturnGarbageForNonExistentKey() throws IOException {
        Piston db = Piston.open();
        ByteString key = ByteString.copyFromUtf8("hello");
        ByteString res = db.get(key);
        assertNull(res);
    }
}
