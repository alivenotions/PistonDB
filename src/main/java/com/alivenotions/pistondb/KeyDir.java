package com.alivenotions.pistondb;

import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class KeyDir {
    Map<ByteString, ByteString> map = new HashMap<ByteString, ByteString>();

    public boolean put(ByteString key, ByteString value) {
        map.put(key, value);
        return true;
    }

    public Optional<ByteString> get(ByteString key) {
        return Optional.ofNullable(map.get(key));
    }
}
