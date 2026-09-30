package io.github.ahmadbitaarr.raftkv.log;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface PersistentLog {
    void append(LogEntry entry) throws IOException;

    Optional<LogEntry> get(long index);

    List<LogEntry> getRange(long fromIndexInclusive, long toIndexInclusive);

    long lastLogIndex();

    /**
     * Structurally removes every log entry with an index greater than {@code index}.
     *
     * <p>This logical abstraction does not decide whether a truncation is allowed by
     * distributed commit policy. Durable implementations may reject a rewrite that
     * would produce internally contradictory stored metadata.</p>
     *
     * @param index index of the final entry to retain; zero clears the entire log
     */
    void truncateAfter(long index) throws IOException;
}
