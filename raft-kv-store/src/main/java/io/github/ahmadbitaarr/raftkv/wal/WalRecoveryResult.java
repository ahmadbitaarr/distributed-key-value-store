package io.github.ahmadbitaarr.raftkv.wal;

import io.github.ahmadbitaarr.raftkv.log.LogEntry;

import java.util.List;

public final class WalRecoveryResult {
    private final List<LogEntry> entries;
    private final long commitIndex;
    private final long lastValidOffset;
    private final long originalFileLength;
    private final boolean tailTruncated;

    public WalRecoveryResult(
            List<LogEntry> entries,
            long commitIndex,
            long lastValidOffset,
            long originalFileLength,
            boolean tailTruncated) {
        this.entries = List.copyOf(entries);
        if (commitIndex < 0) {
            throw new IllegalArgumentException("commitIndex must not be negative");
        }
        if (commitIndex > this.entries.size()) {
            throw new IllegalArgumentException("commitIndex must not exceed lastLogIndex");
        }
        if (lastValidOffset < 0 || originalFileLength < 0) {
            throw new IllegalArgumentException("file offsets must not be negative");
        }
        this.commitIndex = commitIndex;
        this.lastValidOffset = lastValidOffset;
        this.originalFileLength = originalFileLength;
        this.tailTruncated = tailTruncated;
    }

    public List<LogEntry> entries() {
        return entries;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long lastValidOffset() {
        return lastValidOffset;
    }

    public long originalFileLength() {
        return originalFileLength;
    }

    public boolean tailTruncated() {
        return tailTruncated;
    }
}
