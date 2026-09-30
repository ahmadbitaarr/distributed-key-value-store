package io.github.ahmadbitaarr.raftkv.wal;

import io.github.ahmadbitaarr.raftkv.log.LogEntry;

import java.util.Objects;

public final class WalRecord {
    private final WalRecordType type;
    private final LogEntry entry;
    private final long commitIndex;

    private WalRecord(WalRecordType type, LogEntry entry, long commitIndex) {
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.entry = entry;
        this.commitIndex = commitIndex;
    }

    public static WalRecord entry(LogEntry entry) {
        return new WalRecord(WalRecordType.ENTRY,
                Objects.requireNonNull(entry, "entry must not be null"), -1);
    }

    public static WalRecord commit(long commitIndex) {
        if (commitIndex < 0) {
            throw new IllegalArgumentException("commitIndex must not be negative");
        }
        return new WalRecord(WalRecordType.COMMIT, null, commitIndex);
    }

    public WalRecordType type() {
        return type;
    }

    public LogEntry entry() {
        if (type != WalRecordType.ENTRY) {
            throw new IllegalStateException("COMMIT record has no LogEntry");
        }
        return entry;
    }

    public long commitIndex() {
        if (type != WalRecordType.COMMIT) {
            throw new IllegalStateException("ENTRY record has no commitIndex");
        }
        return commitIndex;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof WalRecord record)) {
            return false;
        }
        return type == record.type
                && commitIndex == record.commitIndex
                && Objects.equals(entry, record.entry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, entry, commitIndex);
    }
}
