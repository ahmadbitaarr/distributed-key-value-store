package io.github.ahmadbitaarr.raftkv.wal;

import io.github.ahmadbitaarr.raftkv.log.LogEntry;

import java.io.IOException;
import java.util.List;

public interface WalStore extends AutoCloseable {
    WalRecoveryResult recoveryResult();

    void appendEntryDurably(LogEntry entry) throws IOException;

    void appendCommitDurably(long commitIndex) throws IOException;

    void rewriteDurably(List<LogEntry> retainedEntries, long commitIndex) throws IOException;

    long lastDurableLogIndex();

    long durableCommitIndex();

    @Override
    void close() throws IOException;
}
