package com.alivenotions.pistondb;

import com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

public class Piston {
    KeyDir keyDir;
    DataFile dataFile;

    public static Piston open() throws IOException {
        Piston db = new Piston();
        db.dataFile = DataFile.create(new File("/tmp"));

        KeyDir newKeyDir = new KeyDir();
        db.keyDir = newKeyDir;

        return db;
    }

    public ByteString get(ByteString key) throws IOException {
        Optional<DirEntry> result = keyDir.get(key);
        if (result.isEmpty()) {
            return null;
        }
        return ByteString.empty();
    }

    public void put(ByteString key, ByteString value) throws IOException {
        DirEntry dirEntry = dataFile.write(key, value);
        keyDir.put(key, dirEntry);
    }
}
