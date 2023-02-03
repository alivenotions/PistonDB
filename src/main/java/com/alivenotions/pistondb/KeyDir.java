package com.alivenotions.pistondb;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class KeyDir {
    // TODO: Move to ByteString from com.google.protobuf.ByteString
    // https://stackoverflow.com/questions/29018411/google-protobuf-bytestring-vs-byte
    Map<ByteString, ByteString> map = new HashMap<ByteString, ByteString>();

    public boolean put(ByteString key, ByteString value) {
        map.put(key, value);
        return true;
    }

    public Optional<ByteString> get(ByteString key) {
        return Optional.ofNullable(map.get(key));
    }
}
