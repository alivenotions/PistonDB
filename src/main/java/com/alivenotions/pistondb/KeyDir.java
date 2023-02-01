package com.alivenotions.pistondb;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class KeyDir {
    // TODO: Move to ByteString from com.google.protobuf.ByteString
    // https://stackoverflow.com/questions/29018411/google-protobuf-bytestring-vs-byte
    Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();

    public boolean put(byte[] key, byte[] value) {
        map.put(key, value);
        return true;
    }

    public Optional<byte[]> get(byte[] key) {
        System.out.println("getting a new value");
        System.out.print("The value of key:" + key);
        // This will be null because arrays by default compare reference
        return Optional.ofNullable(map.get(key));
    }
}
