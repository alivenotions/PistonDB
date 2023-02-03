package com.alivenotions.pistondb;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class MainTest {
    @Test
    public void shouldStoreTheValueCorrectly() throws UnsupportedEncodingException, IOException {
        Piston db = Piston.open();
        ByteString key = new ByteString("hello".getBytes("UTF-8"));
        ByteString value = new ByteString("okay".getBytes("UTF-8"));
        db.put(key, value);
        byte[] res = db.get(new ByteString("hello".getBytes("UTF-8"))).getArray();
        String str = new String(res, StandardCharsets.UTF_8);
        assertEquals(str, "okay");
    }
}
