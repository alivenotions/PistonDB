package com.alivenotions.pistondb;

import com.google.protobuf.ByteString;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

public class DataFile implements AutoCloseable {
    // CRC (4) + TIMESTAMP(4) + KEYSIZE(4) + VALUESIZE(4)
    static final int HEADER_SIZE = 16;
    static final int CRC_OFFSET = 0;
    static final int TIMESTAMP_OFFSET = 4;
    static final int KEY_SIZE_OFFSET = 8;
    static final int VALUE_SIZE_OFFSET = 12;
    private FileChannel readChannel;
    private FileChannel writeChannel;
    // Should this be AtomicLong? or volatile? 🤷
    private long offset;

    private DataFile(final FileChannel readChannel, final FileChannel writeChannel) {
        this.readChannel = readChannel;
        this.writeChannel = writeChannel;

        try {
            this.offset = readChannel.size();
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Error trying to read the size of read channel. Hopefully a better way to"
                            + " handle this is coming");
        }
    }

    @SuppressWarnings("resource")
    static DataFile init(final File directory) throws FileNotFoundException {
        if (!directory.exists() || !directory.isDirectory()) {
            throw new IllegalArgumentException("Invalid directory: " + directory.getAbsolutePath());
        }

        File file;
        int timestamp = getTimestamp();

        do {
            file = getFilename(directory, timestamp++);
        } while (file.exists());

        // FIXME: Switching the order causes a FileNotFoundException. Why? And simplify it.
        FileChannel writeChannel = new FileOutputStream(file, true).getChannel();
        FileChannel readChannel = new RandomAccessFile(file, "r").getChannel();

        return new DataFile(readChannel, writeChannel);
    }

    public long write(final ByteString key, final ByteString value, final int timestamp)
            throws IOException {

        int keySize = key.size();
        int valueSize = value.size();

        // TODO: Implement CRC
        int crc = 0xffff;
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        header.putInt(CRC_OFFSET, crc);
        header.putInt(TIMESTAMP_OFFSET, timestamp);
        header.putInt(KEY_SIZE_OFFSET, keySize);
        header.putInt(VALUE_SIZE_OFFSET, valueSize);

        ByteBuffer keyBuffer = key.asReadOnlyByteBuffer();
        ByteBuffer valueBuffer = value.asReadOnlyByteBuffer();
        ByteBuffer[] dataRecord = new ByteBuffer[] {header, keyBuffer, valueBuffer};

        final long head = offset;
        synchronized (Objects.requireNonNull(writeChannel)) {
            // We are using putInt which is an absolute operation, and they don't affect position.
            // Therefore, we do not need to call flip before we write (yay).
            final long bytesWritten = writeChannel.write(dataRecord);
            offset += bytesWritten;
            writeChannel.force(true);
        }

        return head;
    }

    public ByteString read(final long offset) throws IOException {
        if (offset < 0) {
            throw new IllegalArgumentException("offset cannot be negative");
        }
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        readChannel.read(header, offset);

        int keySize = header.getInt(KEY_SIZE_OFFSET);
        int valueSize = header.getInt(VALUE_SIZE_OFFSET);
        ByteBuffer value = ByteBuffer.allocate(valueSize);
        readChannel.read(value, offset + HEADER_SIZE + keySize);

        value.flip();
        return ByteString.copyFrom(value);
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

    // TODO (alivenotions):Move the next three methods to a utility class?
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
