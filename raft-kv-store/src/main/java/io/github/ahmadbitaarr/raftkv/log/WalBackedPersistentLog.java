package io.github.ahmadbitaarr.raftkv.log;

import io.github.ahmadbitaarr.raftkv.wal.FileWalStore;
import io.github.ahmadbitaarr.raftkv.wal.WalRecoveryResult;
import io.github.ahmadbitaarr.raftkv.wal.WalStore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class WalBackedPersistentLog implements PersistentLog, AutoCloseable {
    private final WalStore walStore;
    private final List<LogEntry> entries;

    public WalBackedPersistentLog(WalStore walStore) {
        this.walStore = Objects.requireNonNull(walStore, "walStore must not be null");
        WalRecoveryResult recovery = walStore.recoveryResult();
        this.entries = new ArrayList<>(recovery.entries());
    }

    public static WalBackedPersistentLog open(Path walPath) throws IOException {
        return new WalBackedPersistentLog(FileWalStore.open(walPath));
    }

    @Override
    public void append(LogEntry entry) throws IOException {
        Objects.requireNonNull(entry, "entry must not be null");
        long expectedIndex = lastLogIndex() + 1;
        if (entry.index() != expectedIndex) {
            throw new IllegalArgumentException(
                    "entry index must be contiguous: expected "
                            + expectedIndex
                            + " but was "
                            + entry.index());
        }

        walStore.appendEntryDurably(entry);
        entries.add(entry);
    }

    @Override
    public Optional<LogEntry> get(long index) {
        requirePositiveIndex(index, "index");
        if (index > lastLogIndex()) {
            return Optional.empty();
        }
        return Optional.of(entries.get(toListPosition(index)));
    }

    @Override
    public List<LogEntry> getRange(long fromIndexInclusive, long toIndexInclusive) {
        requirePositiveIndex(fromIndexInclusive, "fromIndexInclusive");
        requirePositiveIndex(toIndexInclusive, "toIndexInclusive");
        if (fromIndexInclusive > toIndexInclusive) {
            throw new IllegalArgumentException("range start must not exceed range end");
        }
        if (toIndexInclusive > lastLogIndex()) {
            throw new IllegalArgumentException("range end exceeds lastLogIndex");
        }
        int fromPosition = toListPosition(fromIndexInclusive);
        int toPositionExclusive = toListPosition(toIndexInclusive) + 1;
        return List.copyOf(entries.subList(fromPosition, toPositionExclusive));
    }

    @Override
    public long lastLogIndex() {
        return entries.size();
    }

    @Override
    public void truncateAfter(long index) throws IOException {
        if (index < 0) {
            throw new IllegalArgumentException("index must not be negative");
        }
        if (index >= lastLogIndex()) {
            return;
        }
        if (index < walStore.durableCommitIndex()) {
            throw new IllegalStateException(
                    "structural truncation cannot produce commitIndex > lastLogIndex");
        }

        List<LogEntry> retained = List.copyOf(entries.subList(0, Math.toIntExact(index)));
        walStore.rewriteDurably(retained, walStore.durableCommitIndex());
        entries.subList(Math.toIntExact(index), entries.size()).clear();
    }

    @Override
    public void close() throws IOException {
        walStore.close();
    }

    private static void requirePositiveIndex(long index, String name) {
        if (index <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private static int toListPosition(long logIndex) {
        return Math.toIntExact(logIndex - 1);
    }
}
