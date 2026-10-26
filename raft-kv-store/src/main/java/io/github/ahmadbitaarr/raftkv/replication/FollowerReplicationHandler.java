package io.github.ahmadbitaarr.raftkv.replication;

import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import io.github.ahmadbitaarr.raftkv.server.NodeRole;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.log.WalBackedPersistentLog;
import io.github.ahmadbitaarr.raftkv.server.NodeStatusSnapshot;
import io.github.ahmadbitaarr.raftkv.wal.WalStore;
import io.github.ahmadbitaarr.raftkv.statemachine.StateMachineApplier;
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
    private final StateMachineApplier applier;
    private final Consumer<Long> publishCommitIndex;
    private final Consumer<Long> publishAppliedIndex;
    private final AtomicReference<IOException> firstFailure = new AtomicReference<>();

    public FollowerReplicationHandler(ClusterConfig config, WalBackedPersistentLog log, WalStore store,
            ReplicationOperationGate gate, Supplier<NodeStatusSnapshot> status,
            Consumer<Long> publishDurableIndex, Consumer<IOException> fatal) {
        this(config,log,store,gate,status,publishDurableIndex,null,ignored->{},ignored->{},fatal);
    }

    /** Phase 9 follower-only commit/application ownership; original constructor remains compatible. */
    public FollowerReplicationHandler(ClusterConfig config, WalBackedPersistentLog log, WalStore store,
            ReplicationOperationGate gate, Supplier<NodeStatusSnapshot> status,
            Consumer<Long> publishDurableIndex, StateMachineApplier applier,
            Consumer<Long> publishCommitIndex, Consumer<Long> publishAppliedIndex, Consumer<IOException> fatal) {
        this.config=Objects.requireNonNull(config);
        if(config.localRole()!=NodeRole.FOLLOWER)throw new IllegalArgumentException("follower only");
        this.log=Objects.requireNonNull(log);this.store=Objects.requireNonNull(store);
        this.gate=Objects.requireNonNull(gate);this.status=Objects.requireNonNull(status);
        this.publishDurableIndex=Objects.requireNonNull(publishDurableIndex);
        this.applier=applier;
        this.publishCommitIndex=Objects.requireNonNull(publishCommitIndex);
        this.publishAppliedIndex=Objects.requireNonNull(publishAppliedIndex);
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

    public record CommitDecision(long commitIndex, long appliedIndex) { }
    public record TruncateDecision(long lastLogIndex, long commitIndex) { }

    /** Shared writer + operation permit covers force, ordered apply and coherent publication. */
    public CommitDecision advanceCommit(String leaderId,long target) throws IOException {
        requireLeader(leaderId);
        if(target<0)throw new IllegalArgumentException("negative commit index");
        if(firstFailure.get()!=null)throw new StorageFailedException(firstFailure.get());
        var permit=gate.tryAcquire();
        if(permit==null)throw new ProtocolRejectedException("follower replication admission closed");
        try(permit) {
            synchronized(writer) {
                requireAvailable();
                if(applier==null)throw new ProtocolRejectedException("commit application not configured");
                if(target>log.lastLogIndex())throw new ProtocolRejectedException("commit beyond durable last index");
                if(target<store.durableCommitIndex())throw new ProtocolRejectedException("commit regression");
                try {
                    if(target>store.durableCommitIndex()) {
                        store.appendCommitDurably(target); // force(true) precedes any publication or application
                        publishCommitIndex.accept(store.durableCommitIndex());
                    }
                    long applied=applier.applyThrough(target,store.durableCommitIndex(),log);
                    publishAppliedIndex.accept(applied);
                    return new CommitDecision(store.durableCommitIndex(),applied);
                } catch(IOException failure) {
                    latchFatal(failure);throw failure;
                } catch(RuntimeException | Error failure) {
                    IOException unsafe=new IOException("follower commit/application outcome unsafe",failure);
                    latchFatal(unsafe);
                    if(failure instanceof Error fatalError)throw fatalError;
                    throw unsafe;
                }
            }
        }
    }

    /** Truncation is only of an uncommitted suffix; leader verifies the retained prefix. */
    public TruncateDecision truncateUncommittedSuffix(String leaderId,long retainThrough) throws IOException {
        requireLeader(leaderId);
        if(retainThrough<0)throw new IllegalArgumentException("negative retained index");
        if(firstFailure.get()!=null)throw new StorageFailedException(firstFailure.get());
        var permit=gate.tryAcquire();
        if(permit==null)throw new ProtocolRejectedException("follower replication admission closed");
        try(permit) {
            synchronized(writer) {
                requireAvailable();
                long committed=store.durableCommitIndex();
                if(retainThrough<committed || (applier!=null && retainThrough<applier.appliedIndex()))
                    throw new ProtocolRejectedException("cannot truncate committed or applied prefix");
                if(retainThrough>log.lastLogIndex())
                    throw new ProtocolRejectedException("retained index exceeds stored prefix");
                try {
                    if(retainThrough<log.lastLogIndex()) {
                        log.truncateAfter(retainThrough); // WAL durable rewrite and recovery first
                        publishDurableIndex.accept(log.lastLogIndex());
                    }
                    return new TruncateDecision(log.lastLogIndex(),store.durableCommitIndex());
                } catch(IOException failure) {
                    latchFatal(failure);throw failure;
                } catch(RuntimeException | Error failure) {
                    IOException unsafe=new IOException("follower WAL rewrite outcome unsafe",failure);
                    latchFatal(unsafe);
                    if(failure instanceof Error fatalError)throw fatalError;
                    throw unsafe;
                }
            }
        }
    }

    private void requireLeader(String id) {
        if(!config.configuredLeaderId().equals(id))throw new UnrecognizedLeaderException();
    }

    private void requireAvailable() throws IOException {
        if(firstFailure.get()!=null)throw new StorageFailedException(firstFailure.get());
        if(status.get().state()!=io.github.ahmadbitaarr.raftkv.server.NodeLifecycleState.CONTROL_ONLY)
            throw new ProtocolRejectedException("follower not available for replication");
    }

    public static final class ProtocolRejectedException extends RuntimeException {
        public ProtocolRejectedException(String message){super(message);}
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
