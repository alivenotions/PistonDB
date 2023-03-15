package com.alivenotions.pistondb;

/*
   ┌──────────┬─────────┬────────────┬─────────────┐
   │ fileName │  offset │  timestamp │  valueSize  │
   └──────────┴─────────┴────────────┴─────────────┘
*/
public record DirEntry(String fileName, long offset, int timestamp, int valueSize) {}
