package com.alivenotions.pistondb;

import java.io.IOException;
import java.util.Optional;

public class Piston {
    KeyDir key_dir;

    public static Piston open() {
        Piston db = new Piston();

        KeyDir new_key_dir = new KeyDir();
        db.key_dir = new_key_dir;

        System.out.println("opened a new pistondb");
        return db;
    }

    public byte[] get(byte[] key) throws IOException {
        Optional<byte[]> result = key_dir.get(key);
        if (result.isEmpty()) {
            return null;
        }
        return result.get();
    }

    public void put(byte[] key, byte[] value) {
        key_dir.put(key, value);
    }
}
