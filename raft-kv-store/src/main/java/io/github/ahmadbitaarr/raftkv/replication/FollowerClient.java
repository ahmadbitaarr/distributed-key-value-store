package io.github.ahmadbitaarr.raftkv.replication;

import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import java.io.IOException;

/** Exactly one attempt, never allocate a new index, decide a quorum or advance commit. */
@FunctionalInterface
public interface FollowerClient extends AutoCloseable {
    enum Kind { ACK, INDEX_GAP, INDEX_CONFLICT, NOT_READY, UNAVAILABLE, DEADLINE_EXCEEDED,
        INVALID_RESPONSE, REMOTE_ERROR, UNKNOWN_TIMEOUT, BUSY }
    record Outcome(Kind kind, long requestedIndex, long lastReportedPersistedIndex,
                   long expectedNextIndex, String detail) {
        public Outcome { if (kind == null || detail == null) throw new IllegalArgumentException("null outcome"); }
    }
    static Outcome ack(long requestedIndex, long lastPersisted) {
        return new Outcome(Kind.ACK, requestedIndex, lastPersisted, 0, "durable ACK");
    }
    Outcome replicate(String followerId, LogEntry entry);
    @Override default void close() throws IOException { }
}
