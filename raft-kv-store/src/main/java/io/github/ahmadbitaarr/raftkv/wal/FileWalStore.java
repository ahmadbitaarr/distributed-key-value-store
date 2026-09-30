package io.github.ahmadbitaarr.raftkv.wal;

import io.github.ahmadbitaarr.raftkv.log.LogEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class FileWalStore implements WalStore {
    @FunctionalInterface
    interface ChannelForcer {
        void force(FileChannel channel) throws IOException;
    }

    @FunctionalInterface
    interface AtomicMover {
        void move(Path source, Path target) throws IOException;
    }

    private final Path walPath;
    private final Path rewriteTempPath;
    private final WalRecordCodec codec;
    private final ChannelForcer forcer;
    private final AtomicMover atomicMover;

    private FileChannel channel;
    private final List<LogEntry> durableEntries = new ArrayList<>();
    private long durableCommitIndex;
    private WalRecoveryResult recoveryResult;
    private boolean closed;

    private FileWalStore(
            Path walPath,
            WalRecordCodec codec,
            ChannelForcer forcer,
            AtomicMover atomicMover) {
        this.walPath = walPath;
        this.rewriteTempPath = rewriteTempPath(walPath);
        this.codec = codec;
        this.forcer = forcer;
        this.atomicMover = atomicMover;
    }

    public static FileWalStore open(Path walPath) throws IOException {
        return openForTest(walPath, productionForcer(), productionAtomicMover());
    }

    static FileWalStore openForTest(
            Path walPath,
            ChannelForcer forcer,
            AtomicMover atomicMover) throws IOException {
        Objects.requireNonNull(walPath, "walPath must not be null");
        Objects.requireNonNull(forcer, "forcer must not be null");
        Objects.requireNonNull(atomicMover, "atomicMover must not be null");

        Path normalized = walPath.toAbsolutePath().normalize();
        FileWalStore store = new FileWalStore(
                normalized, new WalRecordCodec(), forcer, atomicMover);
        try {
            store.openInitialChannel();
            store.recoverFromCurrentChannel();
            store.cleanupOrphanRewriteTemp();
            return store;
        } catch (IOException | RuntimeException exception) {
            store.closeQuietly();
            throw exception;
        }
    }

    static ChannelForcer productionForcer() {
        return channel -> channel.force(true);
    }

    static AtomicMover productionAtomicMover() {
        return (source, target) -> Files.move(
                source,
                target,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    static Path rewriteTempPath(Path walPath) {
        Objects.requireNonNull(walPath, "walPath must not be null");
        Path absolute = walPath.toAbsolutePath().normalize();
        String fileName = absolute.getFileName().toString();
        return absolute.resolveSibling(fileName + ".rewrite.tmp");
    }

    @Override
    public WalRecoveryResult recoveryResult() {
        ensureOpenUnchecked();
        return recoveryResult;
    }

    @Override
    public void appendEntryDurably(LogEntry entry) throws IOException {
        ensureOpen();
        Objects.requireNonNull(entry, "entry must not be null");
        long expectedIndex = lastDurableLogIndex() + 1;
        if (entry.index() != expectedIndex) {
            throw new IllegalArgumentException(
                    "entry index must be contiguous: expected " + expectedIndex + " but was " + entry.index());
        }

        byte[] frame = codec.encode(WalRecord.entry(entry));
        appendFrameDurably(frame);
        durableEntries.add(entry);
        refreshRecoverySnapshot(false, channel.size(), channel.size());
    }

    @Override
    public void appendCommitDurably(long commitIndex) throws IOException {
        ensureOpen();
        if (commitIndex < 0) {
            throw new IllegalArgumentException("commitIndex must not be negative");
        }
        if (commitIndex > lastDurableLogIndex()) {
            throw new IllegalArgumentException("commitIndex must not exceed lastDurableLogIndex");
        }
        if (commitIndex < durableCommitIndex) {
            throw new IllegalArgumentException("commitIndex must not regress");
        }
        if (commitIndex == durableCommitIndex) {
            return;
        }

        byte[] frame = codec.encode(WalRecord.commit(commitIndex));
        appendFrameDurably(frame);
        durableCommitIndex = commitIndex;
        refreshRecoverySnapshot(false, channel.size(), channel.size());
    }

    @Override
    public void rewriteDurably(List<LogEntry> retainedEntries, long commitIndex) throws IOException {
        ensureOpen();
        Objects.requireNonNull(retainedEntries, "retainedEntries must not be null");
        List<LogEntry> canonicalEntries = List.copyOf(retainedEntries);
        validateCanonicalEntries(canonicalEntries);
        if (commitIndex < 0) {
            throw new IllegalArgumentException("commitIndex must not be negative");
        }
        if (commitIndex > canonicalEntries.size()) {
            throw new IllegalStateException("commitIndex cannot exceed retained lastLogIndex");
        }

        buildCanonicalTempWal(canonicalEntries, commitIndex);

        closeCurrentChannel();
        try {
            atomicMover.move(rewriteTempPath, walPath);
        } catch (IOException moveFailure) {
            try {
                reopenOriginalAfterFailedRewrite();
            } catch (IOException reopenFailure) {
                moveFailure.addSuppressed(reopenFailure);
            }
            throw moveFailure;
        }

        try {
            reopenReplacedWal();
            if (!durableEntries.equals(canonicalEntries) || durableCommitIndex != commitIndex) {
                throw new WalCorruptionException("rewritten WAL did not recover expected canonical state");
            }
        } catch (IOException recoveryFailure) {
            throw recoveryFailure;
        }
    }

    @Override
    public long lastDurableLogIndex() {
        ensureOpenUnchecked();
        return durableEntries.size();
    }

    @Override
    public long durableCommitIndex() {
        ensureOpenUnchecked();
        return durableCommitIndex;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        channel = null;
    }

    private void openInitialChannel() throws IOException {
        Path parent = walPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        if (Files.exists(walPath) && Files.isDirectory(walPath)) {
            throw new IOException("WAL path is a directory: " + walPath);
        }
        channel = FileChannel.open(
                walPath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
    }

    private void recoverFromCurrentChannel() throws IOException {
        durableEntries.clear();
        durableCommitIndex = 0;

        long originalLength = channel.size();
        long offset = 0;
        long lastValidOffset = 0;
        boolean tailTruncated = false;

        while (offset < originalLength) {
            long remaining = originalLength - offset;
            if (remaining < WalRecordCodec.HEADER_SIZE) {
                truncateCrashTail(lastValidOffset);
                tailTruncated = true;
                break;
            }

            byte[] header = readExactlyAt(offset, WalRecordCodec.HEADER_SIZE);
            int payloadLength = codec.payloadLengthFromHeader(header);
            long frameLength = (long) WalRecordCodec.HEADER_SIZE
                    + payloadLength
                    + WalRecordCodec.CHECKSUM_SIZE;
            if (remaining < frameLength) {
                truncateCrashTail(lastValidOffset);
                tailTruncated = true;
                break;
            }

            if (frameLength > Integer.MAX_VALUE) {
                throw new WalCorruptionException("WAL frame is too large to decode");
            }
            byte[] frame = readExactlyAt(offset, Math.toIntExact(frameLength));
            WalRecord record = codec.decode(frame);
            applyRecoveredRecord(record);

            offset += frameLength;
            lastValidOffset = offset;
        }

        long repairedLength = channel.size();
        channel.position(repairedLength);
        recoveryResult = new WalRecoveryResult(
                durableEntries,
                durableCommitIndex,
                repairedLength,
                originalLength,
                tailTruncated);
    }

    private void applyRecoveredRecord(WalRecord record) throws WalCorruptionException {
        if (record.type() == WalRecordType.ENTRY) {
            LogEntry entry = record.entry();
            long expectedIndex = durableEntries.size() + 1L;
            if (entry.index() != expectedIndex) {
                throw new WalCorruptionException(
                        "non-contiguous ENTRY index: expected " + expectedIndex + " but was " + entry.index());
            }
            durableEntries.add(entry);
            return;
        }

        long recoveredCommitIndex = record.commitIndex();
        if (recoveredCommitIndex < durableCommitIndex) {
            throw new WalCorruptionException(
                    "COMMIT index regressed from " + durableCommitIndex + " to " + recoveredCommitIndex);
        }
        if (recoveredCommitIndex > durableEntries.size()) {
            throw new WalCorruptionException(
                    "COMMIT index " + recoveredCommitIndex + " exceeds recovered lastLogIndex " + durableEntries.size());
        }
        durableCommitIndex = recoveredCommitIndex;
    }

    private void appendFrameDurably(byte[] frame) throws IOException {
        long startOffset = channel.size();
        try {
            channel.position(startOffset);
            writeFully(channel, ByteBuffer.wrap(frame));
            forcer.force(channel);
        } catch (IOException failure) {
            try {
                channel.truncate(startOffset);
                channel.position(startOffset);
            } catch (IOException rollbackFailure) {
                failure.addSuppressed(rollbackFailure);
            }
            throw failure;
        }
    }

    private void buildCanonicalTempWal(List<LogEntry> entries, long commitIndex) throws IOException {
        Files.deleteIfExists(rewriteTempPath);
        try (FileChannel tempChannel = FileChannel.open(
                rewriteTempPath,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW)) {
            for (LogEntry entry : entries) {
                writeFully(tempChannel, ByteBuffer.wrap(codec.encode(WalRecord.entry(entry))));
            }
            if (commitIndex > 0) {
                writeFully(tempChannel, ByteBuffer.wrap(codec.encode(WalRecord.commit(commitIndex))));
            }
            forcer.force(tempChannel);
        } catch (IOException | RuntimeException failure) {
            try {
                Files.deleteIfExists(rewriteTempPath);
            } catch (IOException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private void reopenOriginalAfterFailedRewrite() throws IOException {
        if (!Files.exists(walPath)) {
            throw new IOException("atomic WAL replacement failed and original WAL path is missing: " + walPath);
        }
        channel = FileChannel.open(walPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
        recoverFromCurrentChannel();
    }

    private void reopenReplacedWal() throws IOException {
        if (!Files.exists(walPath)) {
            throw new IOException("atomic WAL replacement reported success but WAL path is missing");
        }
        channel = FileChannel.open(walPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
        recoverFromCurrentChannel();
    }

    private void truncateCrashTail(long lastValidOffset) throws IOException {
        channel.truncate(lastValidOffset);
        channel.position(lastValidOffset);
        forcer.force(channel);
    }

    private byte[] readExactlyAt(long offset, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        channel.position(offset);
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read < 0) {
                throw new IOException("unexpected EOF while reading WAL");
            }
        }
        return buffer.array();
    }

    private static void writeFully(FileChannel target, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            target.write(buffer);
        }
    }

    private void validateCanonicalEntries(List<LogEntry> entries) {
        for (int i = 0; i < entries.size(); i++) {
            LogEntry entry = Objects.requireNonNull(entries.get(i), "retainedEntries must not contain null");
            long expected = i + 1L;
            if (entry.index() != expected) {
                throw new IllegalArgumentException(
                        "retainedEntries must be contiguous from index 1: expected "
                                + expected + " but was " + entry.index());
            }
        }
    }

    private void cleanupOrphanRewriteTemp() {
        try {
            Files.deleteIfExists(rewriteTempPath);
        } catch (IOException ignored) {
            // The recovered real WAL remains authoritative even if stale-temp cleanup fails.
        }
    }

    private void refreshRecoverySnapshot(boolean tailTruncated, long lastValidOffset, long originalFileLength) {
        recoveryResult = new WalRecoveryResult(
                durableEntries,
                durableCommitIndex,
                lastValidOffset,
                originalFileLength,
                tailTruncated);
    }

    private void closeCurrentChannel() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        channel = null;
    }

    private void closeQuietly() {
        try {
            if (channel != null) {
                channel.close();
            }
        } catch (IOException ignored) {
        } finally {
            channel = null;
            closed = true;
        }
    }

    private void ensureOpen() throws IOException {
        if (closed || channel == null || !channel.isOpen()) {
            throw new IOException("WAL store is closed");
        }
    }

    private void ensureOpenUnchecked() {
        if (closed || channel == null || !channel.isOpen()) {
            throw new IllegalStateException("WAL store is closed");
        }
    }
}
