package io.github.ahmadbitaarr.raftkv.log;

import java.util.List;
import java.util.Optional;

public interface PersistentLog {
    void append(LogEntry entry);

    Optional<LogEntry> get(long index);

    List<LogEntry> getRange(long fromIndexInclusive, long toIndexInclusive);

    long lastLogIndex();

    /**
     * Structurally removes every log entry with an index greater than {@code index}.
     *
     * <p>This Phase 2 abstraction has no commit semantics. Callers in later phases
     * are responsible for ensuring truncation does not remove committed entries.</p>
     *
     * @param index index of the final entry to retain; zero clears the entire log
     */
    void truncateAfter(long index);
}
