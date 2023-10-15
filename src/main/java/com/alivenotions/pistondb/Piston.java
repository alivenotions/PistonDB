package com.alivenotions.pistondb;

import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import java.util.Optional;

public class Piston {
    KeyDir keyDir;
    DataFile dataFile;

    public static Piston open(final File directory) throws IOException {
        Piston db = new Piston();
        db.dataFile = DataFile.init(directory);

        db.keyDir = new KeyDir();

        return db;
    }

    public Optional<ByteString> get(ByteString key) {
        Optional<DirEntry> result = keyDir.get(key);
        return result.map(
                r -> {
                    try {
                        return dataFile.read(r.offset());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    public void put(ByteString key, ByteString value) throws IOException {
        int timestamp = DataFile.getTimestamp();
        long writeOffset = dataFile.write(key, value, timestamp);
        DirEntry dirEntry = new DirEntry(dataFile.toString(), writeOffset, timestamp, value.size());
        keyDir.put(key, dirEntry);
    }

    public void close() throws IOException {
        dataFile.close();
    }
}
