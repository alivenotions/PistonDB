package com.alivenotions.pistondb;

import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class KeyDir {
    Map<ByteString, DirEntry> map = new HashMap<>();

    public void put(ByteString key, DirEntry value) {
        map.put(key, value);
    }

    public Optional<DirEntry> get(ByteString key) {
        return Optional.ofNullable(map.get(key));
    }
}
