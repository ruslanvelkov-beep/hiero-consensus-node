// SPDX-License-Identifier: Apache-2.0
package com.swirlds.merkledb.files;

import static com.swirlds.merkledb.files.DataFileCommon.FIELD_DATAFILE_ITEMS;

import com.hedera.pbj.runtime.ProtoWriterTools;
import com.hedera.pbj.runtime.io.buffer.BufferedData;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.time.Instant;
import java.util.function.Consumer;
import org.hiero.base.utility.MemoryUtils;

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
public final class MmapDataFileWriter extends DataFileWriter {

    private static final String ERROR_DATA_ITEM_TOO_LARGE =
            "Data item is too large to write to a data file. Increase data file mapped byte buffer size";

    /**
     * The current mapped byte buffer used for writing. When overflowed, it is released, and another
     * buffer is mapped from the file channel.
     */
    private MappedByteBuffer mappedDataBuffer;

    /**
     * Offset, in bytes, of the current mapped byte buffer in the file channel.
     */
    private long bufferPositionInFile;

    private BufferedData dataBuffer;

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
    public MmapDataFileWriter(
            final String filePrefix,
            final Path dataFileDir,
            final int index,
            final Instant creationTime,
            final int compactionLevel)
            throws IOException {
        super(filePrefix, dataFileDir, index, creationTime, compactionLevel);

        bufferPositionInFile = writeHeader();
        moveWritingBuffer(bufferPositionInFile);
    }

    private long getCurrentFilePosition() {
        return bufferPositionInFile + dataBuffer.position();
    }

    /**
     * Maps the writing byte buffer to the given position in the file. Byte buffer size is always
     * {@link #DEFAULT_BUF_SIZE}. Previous mapped byte buffer, if not null, is released.
     *
     * @param startPosition new mapped byte buffer position in the file, in bytes
     * @throws IOException if I/O error(s) occurred
     */
    private void moveWritingBuffer(final long startPosition) throws IOException {
        final MappedByteBuffer newBuffer = fileChannel.map(MapMode.READ_WRITE, startPosition, DEFAULT_BUF_SIZE);
        if (mappedDataBuffer != null) {
            MemoryUtils.closeMmapBuffer(mappedDataBuffer);
        }
        bufferPositionInFile = startPosition;
        mappedDataBuffer = newBuffer;
        dataBuffer = BufferedData.wrap(mappedDataBuffer);
    }

    @Override
    protected long storeDataItemImpl(final Consumer<BufferedData> dataItemWriter, final int dataItemSize) throws IOException {
        final long fileOffset = getCurrentFilePosition();
        final int sizeToWrite = ProtoWriterTools.sizeOfDelimited(FIELD_DATAFILE_ITEMS, dataItemSize);

        if (sizeToWrite > DEFAULT_BUF_SIZE) {
            throw new IOException(
                    ERROR_DATA_ITEM_TOO_LARGE + " dataSize=" + sizeToWrite + ", bufferSize=" + DEFAULT_BUF_SIZE);
        }

        // if there is not enough space in the current mapped buffer,
        // we need to move it to start at the current file offset
        if (dataBuffer.remaining() < sizeToWrite) {
            moveWritingBuffer(fileOffset);
        }

        // write actual data
        ProtoWriterTools.writeDelimited(dataBuffer, FIELD_DATAFILE_ITEMS, dataItemSize, dataItemWriter);

        // double check that we wrote the expected number of bytes
        if (getCurrentFilePosition() != fileOffset + sizeToWrite) {
            throw new IOException("Estimated size / written bytes mismatch: expected=" + sizeToWrite + " written="
                    + (getCurrentFilePosition() - fileOffset));
        }


        return fileOffset;
    }

    @Override
    protected long storeDataItemWithTagImpl(final BufferedData dataItemWithTag) throws IOException {
        final long fileOffset = getCurrentFilePosition();
        final int sizeToWrite = Math.toIntExact(dataItemWithTag.remaining());

        if (sizeToWrite > DEFAULT_BUF_SIZE) {
            throw new IOException(
                    ERROR_DATA_ITEM_TOO_LARGE + " dataSize=" + sizeToWrite + ", bufferSize=" + DEFAULT_BUF_SIZE);
        }

        // if there is not enough space in the current mapped buffer,
        // we need to move it to start at the current file offset
        if (dataBuffer.remaining() < sizeToWrite) {
            moveWritingBuffer(fileOffset);
        }

        // write actual data
        dataBuffer.writeBytes(dataItemWithTag);

        // double check that we wrote the expected number of bytes
        if (getCurrentFilePosition() != fileOffset + sizeToWrite) {
            throw new IOException("Estimated size / written bytes mismatch: expected=" + sizeToWrite + " written="
                    + (getCurrentFilePosition() - fileOffset));
        }

        return fileOffset;
    }

    @Override
    protected long getFinalFileSize() {
        return bufferPositionInFile + dataBuffer.position();
    }
}
