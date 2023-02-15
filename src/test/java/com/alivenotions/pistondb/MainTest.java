package com.alivenotions.pistondb;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.junit.Test;

public class MainTest {
    @Test
    public void shouldStoreTheValueCorrectly() throws UnsupportedEncodingException, IOException {
        Piston db = Piston.open();
        ByteString key = ByteString.copyFrom("hello", "UTF-8");
        ByteString value = ByteString.copyFrom("okay", "UTF-8");
        db.put(key, value);
        ByteString k2 = ByteString.copyFrom("hello", "UTF-8");
        ByteString res = db.get(k2);
        String str = res.toStringUtf8();
        assertEquals("okay", str);
    }

    @Test
    public void shouldNotReturnGarbageForNonExistentKey() throws IOException {
        Piston db = Piston.open();
        ByteString key = ByteString.copyFrom("hello", "UTF-8");
        ByteString res = db.get(key);
        assertNull(res);
    }
}
