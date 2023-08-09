package com.alivenotions.pistondb;

import com.google.protobuf.ByteString;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DataFile implements AutoCloseable {
    // CRC (4) + TIMESTAMP(4) + KEYSIZE(4) + VALUESIZE(4)
    final int HEADER_SIZE = 16;
    private final int timestamp;
    private final File fileName;
    private FileChannel readChannel;
    private FileChannel writeChannel;
    // Should this be AtomicLong? or volatile? 🤷
    private long offset;

    private DataFile(
            final int timestamp,
            final File fileName,
            final FileChannel readChannel,
            final FileChannel writeChannel)
            throws IOException {
        this.timestamp = timestamp;
        this.fileName = fileName;
        this.readChannel = readChannel;
        this.writeChannel = writeChannel;
        // FIXME: I really don't like that I have to do this.
        // Is there a better way to do this? It will throw an IOException if someone
        // passed a null readchannel.
        this.offset = readChannel.size();
    }

    static DataFile create(final File directory) throws IOException {
        if (!directory.exists() || !directory.isDirectory()) {
            throw new IllegalArgumentException("Invalid directory: " + directory.getAbsolutePath());
        }

        File file;
        int timestamp = getTimestamp();
        System.out.println("timestamp:" + timestamp);

        // Woah! when was the last time a do-while was caught in the wild!
        do {
            file = getFilename(directory, timestamp++);
        } while (file.exists());

        FileChannel writeChannel = new FileOutputStream(file, true).getChannel();
        FileChannel readChannel = new RandomAccessFile(file, "r").getChannel();

        return new DataFile(timestamp, file, readChannel, writeChannel);
    }

    static DataFile open(final File fileName) throws FileNotFoundException, IOException {
        int timestamp = getTimestamp(fileName);

        FileChannel readChannel = new RandomAccessFile(fileName, "r").getChannel();
        FileChannel writeChannel = new FileOutputStream(fileName, true).getChannel();
        return new DataFile(timestamp, fileName, readChannel, writeChannel);
    }

    public DirEntry write(final ByteString key, final ByteString value) throws IOException {
        if (writeChannel == null) {
            throw new IllegalStateException("Data file doesn't have an open channel for writing");
        }

        int timestamp = getTimestamp();
        int keySize = key.size();
        int valueSize = value.size();

        // TODO: Implement CRC
        int crc = 0xffff;
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        header.putInt(0, crc);
        header.putInt(4, timestamp);
        header.putInt(8, keySize);
        header.putInt(12, valueSize);
        header.flip();

        ByteBuffer keyBuffer = key.asReadOnlyByteBuffer();
        ByteBuffer valueBuffer = value.asReadOnlyByteBuffer();

        ByteBuffer[] dataRecord = new ByteBuffer[] {header, keyBuffer, valueBuffer};
        int entrySize = HEADER_SIZE + keySize + valueSize;
        synchronized (writeChannel) {
            writeChannel.write(dataRecord);
            // does synchronized work like this?
            offset += entrySize;
        }

        return new DirEntry(fileName.toString(), offset, timestamp, valueSize);
    }

    @Override
    public void close() throws IOException {
        closeForWriting();
        if (readChannel != null) {
            readChannel.close();
            readChannel = null;
        }
    }

    public void closeForWriting() throws IOException {
        if (writeChannel != null) {
            writeChannel.close();
            writeChannel = null;
        }
    }

    static File getFilename(File dirname, int timestamp) {
        return new File(dirname, timestamp + ".data");
    }

    static int getTimestamp() {
        return (int) System.currentTimeMillis() / 1000;
    }

    static int getTimestamp(File fileName) {
        int firstIndexOfPeriod = fileName.getName().indexOf('.');
        return Integer.parseInt(fileName.getName().substring(0, firstIndexOfPeriod));
    }
}
