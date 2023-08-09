package com.alivenotions.pistondb;

import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import org.junit.Test;
import static org.junit.Assert.*;

public class DataFileTest {

  @Test
  public void testCreateAndWrite() throws IOException {
    File directory = new File("data");
    DataFile dataFile = DataFile.create(directory);

    ByteString key = ByteString.copyFromUtf8("key");
    ByteString value = ByteString.copyFromUtf8("value");

    DirEntry dirEntry = dataFile.write(key, value);

    assertNotNull(dirEntry);
    assertEquals(key.size(), dirEntry.valueSize());
  }

  @Test
  public void testOpenAndWrite() throws IOException {
    File file = new File("data/123456.data"); // Assuming the file exists
    DataFile dataFile = DataFile.open(file);

    ByteString key = ByteString.copyFromUtf8("key");
    ByteString value = ByteString.copyFromUtf8("value");

    DirEntry dirEntry = dataFile.write(key, value);

    assertNotNull(dirEntry);
    assertEquals(key.size(), dirEntry.valueSize());
  }
}
