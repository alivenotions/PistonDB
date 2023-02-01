package com.alivenotions.pistondb;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class Main {
    public static void main(String[] args) throws UnsupportedEncodingException, IOException {
        Piston db = Piston.open();
        db.put("hello".getBytes("UTF-8"), "okay".getBytes("UTF-8"));
        System.out.println(db.get("hello".getBytes("UTF-8")));
    }
}
