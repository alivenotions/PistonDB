package com.alivenotions.pistondb;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Optional;

public class Piston {
    KeyDir keyDir;

    public static Piston open() {
        Piston db = new Piston();

        KeyDir newKeyDir = new KeyDir();
        db.keyDir = newKeyDir;

        return db;
    }

    public ByteString get(ByteString key) throws IOException {
        Optional<ByteString> result = keyDir.get(key);
        if (result.isEmpty()) {
            return null;
        }
        return result.get();
    }

    public void put(ByteString key, ByteString value) {
        // Write to file -> pos, fileId
        // Create a DirEntry
        // Write to keyDir
        keyDir.put(key, value);
    }
}
