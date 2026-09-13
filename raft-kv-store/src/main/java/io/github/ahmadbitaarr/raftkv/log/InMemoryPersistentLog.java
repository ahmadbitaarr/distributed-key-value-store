package io.github.ahmadbitaarr.raftkv.log;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class InMemoryPersistentLog implements PersistentLog {
    private final List<LogEntry> entries = new ArrayList<>();

    @Override
    public void append(LogEntry entry) {
        Objects.requireNonNull(entry, "entry must not be null");

        long expectedIndex = lastLogIndex() + 1;
        if (entry.index() != expectedIndex) {
            throw new IllegalArgumentException(
                    "entry index must be contiguous: expected "
                            + expectedIndex
                            + " but was "
                            + entry.index());
        }

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
    public void truncateAfter(long index) {
        if (index < 0) {
            throw new IllegalArgumentException("index must not be negative");
        }

        if (index >= lastLogIndex()) {
            return;
        }

        entries.subList((int) index, entries.size()).clear();
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
