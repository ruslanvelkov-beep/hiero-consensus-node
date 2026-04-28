// SPDX-License-Identifier: Apache-2.0
package com.swirlds.merkledb.files;

import static com.swirlds.merkledb.files.DataFileCommon.FIELD_DATAFILE_ITEMS;

import com.hedera.pbj.runtime.ProtoWriterTools;
import com.hedera.pbj.runtime.io.buffer.BufferedData;
import com.swirlds.merkledb.utilities.MerkleDbFileUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Class for creating and sequentially writing to the file. A data file contains a header
 * containing {@link DataFileMetadata} followed by data items. Each data item is considered
 * as a black box.
 *
 * <p>{@link #close()} must be called after done writing data using {@link #storeDataItem(BufferedData)},
 * or {@link #storeDataItem(Consumer, int)}, or {@link #storeDataItemWithTag(BufferedData)}
 * any number of times. The implementation doesn't control the file size.
 *
 * <p>Internally, the data items are written to a memory mapped file using {@link MappedByteBuffer}
 * of fixed size, that could be provided in constructor. This buffer is moved to the current
 * file position when needed.
 *
 * <p><b>This class is NOT thread safe.</b>
 *
 * <p>{@link DataFileReader} or {@link DataFileIterator} can be used to read file back and access data items.
 */
public final class PwriteDataFileWriter extends DataFileWriter {

    private static final ThreadLocal<ByteBuffer> BUFFER_CACHE = new ThreadLocal<>();
    private static final ThreadLocal<BufferedData> WRITER_CACHE = new ThreadLocal<>();

    /**
     * Offset, in bytes, of the current mapped byte buffer in the file channel.
     */
    private final AtomicLong bufferPositionInFile = new AtomicLong(0);

    /**
     * Create a new data file in the given directory, in append mode. Puts the object into "writing"
     * mode (i.e. creates a lock file. So you'd better start writing data and be sure to finish it
     * off).
     *
     * @param filePrefix string prefix for all files, must not contain "_" chars
     * @param dataFileDir the path to directory to create the data file in
     * @param index the index number for this file
     * @param creationTime the time stamp for the creation time for this file
     * @param compactionLevel the compaction level for this file
     */
    public PwriteDataFileWriter(
            final String filePrefix,
            final Path dataFileDir,
            final int index,
            final Instant creationTime,
            final int compactionLevel)
            throws IOException {
        super(filePrefix, dataFileDir, index, creationTime, compactionLevel);

        final long headerLen = writeHeader();
        bufferPositionInFile.set(headerLen);
    }

    @Override
    protected long storeDataItemImpl(final Consumer<BufferedData> dataItemWriter, final int dataItemSize) throws IOException {
        final int sizeToWrite = ProtoWriterTools.sizeOfDelimited(FIELD_DATAFILE_ITEMS, dataItemSize);
        final long fileOffset = bufferPositionInFile.getAndAdd(sizeToWrite);

        ByteBuffer writeBytes = BUFFER_CACHE.get();
        final BufferedData writeBuffer;
        if ((writeBytes == null) || (writeBytes.capacity() < sizeToWrite)) {
            writeBytes = ByteBuffer.allocate(sizeToWrite);
            BUFFER_CACHE.set(writeBytes);
            writeBuffer = BufferedData.wrap(writeBytes);
            WRITER_CACHE.set(writeBuffer);
        } else {
            writeBuffer = WRITER_CACHE.get();
            writeBuffer.position(0);
            writeBuffer.limit(sizeToWrite);
        }

        // write actual data
        ProtoWriterTools.writeDelimited(writeBuffer, FIELD_DATAFILE_ITEMS, dataItemSize, dataItemWriter);
        assert writeBuffer.position() == sizeToWrite;

        // write to the file
        writeBytes.flip();
        final int bytesWritten = MerkleDbFileUtils.completelyWrite(fileChannel, writeBytes, fileOffset);
        assert bytesWritten == sizeToWrite;

        return fileOffset;
    }

    @Override
    protected long storeDataItemWithTagImpl(final BufferedData dataItemWithTag) throws IOException {
        final int sizeToWrite = Math.toIntExact(dataItemWithTag.remaining());
        final long fileOffset = bufferPositionInFile.getAndAdd(sizeToWrite);

        ByteBuffer writeBytes = BUFFER_CACHE.get();
        final BufferedData writeBuffer;
        if ((writeBytes == null) || (writeBytes.capacity() < sizeToWrite)) {
            writeBytes = ByteBuffer.allocate(sizeToWrite);
            BUFFER_CACHE.set(writeBytes);
            writeBuffer = BufferedData.wrap(writeBytes);
            WRITER_CACHE.set(writeBuffer);
        } else {
            writeBuffer = WRITER_CACHE.get();
            writeBuffer.position(0);
            writeBuffer.limit(sizeToWrite);
        }

        // write actual data
        writeBuffer.writeBytes(dataItemWithTag);
        assert writeBuffer.position() == sizeToWrite;

        // write to the file
        writeBytes.flip();
        final int bytesWritten = MerkleDbFileUtils.completelyWrite(fileChannel, writeBytes, fileOffset);
        assert bytesWritten == sizeToWrite;

        return fileOffset;
    }

    @Override
    protected long getFinalFileSize() {
        return bufferPositionInFile.get();
    }
}
