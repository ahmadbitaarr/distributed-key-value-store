package io.github.ahmadbitaarr.raftkv.replication;

import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import io.github.ahmadbitaarr.raftkv.server.NodeRole;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.log.WalBackedPersistentLog;
import io.github.ahmadbitaarr.raftkv.server.NodeStatusSnapshot;
import io.github.ahmadbitaarr.raftkv.wal.WalStore;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Performs one atomic decision+durable-append, borrowing the node's single WAL owner. */
public final class FollowerReplicationHandler {
    public enum Kind { ACCEPTED, INDEX_GAP, INDEX_CONFLICT, NOT_READY }
    public record Decision(Kind kind, long lastPersistedIndex, long commitIndex, long expectedNextIndex) {
        public boolean accepted() { return kind == Kind.ACCEPTED; }
    }
    private final ClusterConfig config;
    private final Object writer = new Object();
    private final WalBackedPersistentLog log;
    private final WalStore store;
    private final ReplicationOperationGate gate;
    private final Supplier<NodeStatusSnapshot> status;
    private final Consumer<Long> publishDurableIndex;
    private final Consumer<IOException> fatal;
    private final AtomicReference<IOException> firstFailure = new AtomicReference<>();

    public FollowerReplicationHandler(ClusterConfig config, WalBackedPersistentLog log, WalStore store,
            ReplicationOperationGate gate, Supplier<NodeStatusSnapshot> status,
            Consumer<Long> publishDurableIndex, Consumer<IOException> fatal) {
        this.config=Objects.requireNonNull(config);
        if (config.localRole()!=NodeRole.FOLLOWER)
            throw new IllegalArgumentException("only configured followers may construct replication handler");
        this.log=Objects.requireNonNull(log); this.store=Objects.requireNonNull(store);
        this.gate=Objects.requireNonNull(gate); this.status=Objects.requireNonNull(status);
        this.publishDurableIndex=Objects.requireNonNull(publishDurableIndex);
        this.fatal=Objects.requireNonNull(fatal);
    }

    public IOException storageFailure() { return firstFailure.get(); }

    public Decision replicate(String claimedLeaderId, LogEntry entry) throws IOException {
        Objects.requireNonNull(entry, "entry");
        if (!config.configuredLeaderId().equals(claimedLeaderId))
            throw new UnrecognizedLeaderException();
        if (firstFailure.get() != null) throw new StorageFailedException(firstFailure.get());
        var permit = gate.tryAcquire();
        if (permit == null) {
            if (firstFailure.get() != null) throw new StorageFailedException(firstFailure.get());
            var snap = status.get();
            return new Decision(Kind.NOT_READY, snap.lastLogIndex(), snap.durableCommitIndex(), 0);
        }
        try (permit) {
            synchronized (writer) {
                if (firstFailure.get() != null) throw new StorageFailedException(firstFailure.get());
                var snap = status.get();
                if (snap.state()!=io.github.ahmadbitaarr.raftkv.server.NodeLifecycleState.CONTROL_ONLY)
                    return new Decision(Kind.NOT_READY, snap.lastLogIndex(), snap.durableCommitIndex(), 0);
                try {
                    long last = log.lastLogIndex();
                    long commit = store.durableCommitIndex();
                    if (entry.index() <= last) {
                        boolean identical = log.get(entry.index()).orElseThrow().equals(entry);
                        if (identical) return new Decision(Kind.ACCEPTED, last, commit, 0);
                        if (last == Long.MAX_VALUE) throw new IllegalStateException("log index exhausted");
                        return new Decision(Kind.INDEX_CONFLICT, last, commit, last + 1);
                    }
                    if (last == Long.MAX_VALUE) throw new IllegalStateException("log index exhausted");
                    if (entry.index() > last + 1) return new Decision(Kind.INDEX_GAP, last, commit, last + 1);
                    log.append(entry); // WAL force(true) must complete before return.
                    publishDurableIndex.accept(log.lastLogIndex());
                    return new Decision(Kind.ACCEPTED, log.lastLogIndex(), store.durableCommitIndex(), 0);
                } catch (IOException e) {
                    latchFatal(e);
                    throw e;
                } catch (RuntimeException e) {
                    IOException unsafe = new IOException("unexpected follower WAL state/update failure", e);
                    latchFatal(unsafe);
                    throw unsafe;
                }

            }
        }
    }

    private void latchFatal(IOException error) {
        if (firstFailure.compareAndSet(null, error)) {
            gate.closeAdmission();
            fatal.accept(error); // CAS callback; MUST NEVER acquire NodeServer's lifecycle monitor
        }
    }

    public static final class UnrecognizedLeaderException extends RuntimeException {
        public UnrecognizedLeaderException() { super("unrecognized configured leader ID"); }
    }
    public static final class StorageFailedException extends IOException {
        public StorageFailedException(IOException cause) { super("follower storage previously failed", cause); }
    }
}
