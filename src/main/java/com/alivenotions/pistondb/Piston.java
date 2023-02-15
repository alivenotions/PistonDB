package com.alivenotions.pistondb;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Optional;

public class Piston {
    KeyDir key_dir;

    public static Piston open() {
        Piston db = new Piston();

        KeyDir new_key_dir = new KeyDir();
        db.key_dir = new_key_dir;

        return db;
    }

    public ByteString get(ByteString key) throws IOException {
        Optional<ByteString> result = key_dir.get(key);
        if (result.isEmpty()) {
            return null;
        }
        return result.get();
    }

    public void put(ByteString key, ByteString value) {
        key_dir.put(key, value);
    }
}
