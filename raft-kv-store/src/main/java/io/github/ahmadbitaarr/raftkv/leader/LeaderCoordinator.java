package io.github.ahmadbitaarr.raftkv.leader;

import io.github.ahmadbitaarr.raftkv.commit.AckTracker;
import io.github.ahmadbitaarr.raftkv.commit.CommitManager;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.replication.*;
import io.github.ahmadbitaarr.raftkv.server.NodeRole;
import io.github.ahmadbitaarr.raftkv.statemachine.StateMachineApplier;
import io.github.ahmadbitaarr.raftkv.wal.WalStore;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/** Single-flight, fresh-leader only: durable ENTRY -> prefix majority -> durable COMMIT -> apply. */
public final class LeaderCoordinator {
    public enum Code { OK, NOT_READY, NO_QUORUM }
    public record MutationResult(Code code, long commitIndex) {
        public MutationResult {
            Objects.requireNonNull(code);
            if ((code == Code.OK) != (commitIndex > 0))
                throw new IllegalArgumentException("success requires positive committed index");
        }
    }
    private final ClusterConfig config;
    private final WalBackedPersistentLog log;
    private final WalStore wal;
    private final ReplicationManager replication;
    private final FollowerStatusProbe probe;
    private final AckTracker tracker;
    private final CommitManager commit;
    private final StateMachineApplier applier;
    private final LongConsumer publishDurableIndex;
    private final LongConsumer publishAppliedIndex;
    private final Consumer<IOException> recordFault;
    private final LeaderMutationGate gate=new LeaderMutationGate();
    private final Set<String> admittedIds=new HashSet<>(); // one writer, includes unresolved
    private final AtomicReference<IOException> storageFault=new AtomicReference<>();
    private final boolean freshAtStartup;
    private volatile boolean unresolvedSuffix;

    public LeaderCoordinator(ClusterConfig config, WalBackedPersistentLog log, WalStore wal,
            ReplicationManager replication, FollowerStatusProbe probe, AckTracker tracker,
            CommitManager commit, StateMachineApplier applier,
            LongConsumer publishDurableIndex, LongConsumer publishAppliedIndex,
            Consumer<IOException> recordFault) {
        this.config=Objects.requireNonNull(config);this.log=Objects.requireNonNull(log);
        this.wal=Objects.requireNonNull(wal);this.replication=Objects.requireNonNull(replication);
        this.probe=Objects.requireNonNull(probe);this.tracker=Objects.requireNonNull(tracker);
        this.commit=Objects.requireNonNull(commit);this.applier=Objects.requireNonNull(applier);
        this.publishDurableIndex=Objects.requireNonNull(publishDurableIndex);
        this.publishAppliedIndex=Objects.requireNonNull(publishAppliedIndex);
        this.recordFault=Objects.requireNonNull(recordFault);
        if(config.localRole()!=NodeRole.LEADER)throw new IllegalArgumentException("configured leader only");
        freshAtStartup=log.lastLogIndex()==0 && wal.lastDurableLogIndex()==0 && wal.durableCommitIndex()==0;
    }

    /** Called by NodeServer after gRPC startup; recovered nonempty leader never enables writes. */
    public void openAfterStartup() {if(freshAtStartup)gate.open();}
    public void closeAdmission() {gate.closeAdmission();}
    public boolean hasUnresolvedSuffix(){return unresolvedSuffix;}
    public IOException storageFailure(){return storageFault.get();}
    public boolean isActive(){return gate.isActive();}
    public boolean awaitDrained(long timeout, TimeUnit unit) throws InterruptedException {
        return gate.awaitDrained(timeout,unit);
    }

    public MutationResult mutate(String requestId, Command command) throws IOException {
        Objects.requireNonNull(requestId,"requestId");Objects.requireNonNull(command,"command");
        var permit=gate.tryAcquire();
        if(permit==null) return new MutationResult(Code.NOT_READY,0);
        try(permit) {
            if(storageFault.get()!=null || unresolvedSuffix || admittedIds.contains(requestId))
                return new MutationResult(Code.NOT_READY,0);
            // Explicit joint network decision deadline: at most two bounded status probes
            // plus one concurrent replication fanout. A late ACK never reopens this decision.
            long networkDeadline=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(
                    2L*config.statusDeadlineMs()+config.replicationDeadlineMs()+250L);
            // Probe only previously unknown followers. Nonempty/unavailable history NEVER implies prefix zero.
            for(String follower:config.members().keySet()) {
                if(System.nanoTime()>=networkDeadline)break;
                if(follower.equals(config.configuredLeaderId()) || tracker.verifiedMatchingThrough(follower)>=0)continue;
                try {probe.probeStatus(follower).ifPresent(status -> tracker.certifyEmpty(follower,status));}
                catch(RuntimeException peerUnavailable) { /* unknown is not a quorum certificate */ }
            }
            boolean localEntryDurable=false;
            try {
                long previous=log.lastLogIndex();
                if(previous==Long.MAX_VALUE || wal.lastDurableLogIndex()!=previous
                        || wal.durableCommitIndex()!=previous)
                    throw new IOException("leader has unresolved or inconsistent WAL prefix");
                long next=previous+1;
                // Record before WAL I/O so even an uncertain persisted outcome cannot be retried as new.
                admittedIds.add(requestId);
                LogEntry entry=new LogEntry(next,requestId,command,CanonicalCommandFingerprint.of(command));
                log.append(entry); // leader ENTRY force(true)
                localEntryDurable=true;
                publishDurableIndex.accept(next);

                // One global budget for both follower attempts; slow peer cannot obstruct fast peer.
                ReplicationManager.BatchResult outcomes=replication.replicateStoredEntryUntil(next,networkDeadline);
                for(var accepted:outcomes.outcomes().entrySet()) {
                    tracker.recordDurableAck(accepted.getKey(),entry,accepted.getValue());
                }
                Optional<AckTracker.MajorityCertificate> majority=tracker.majorityFor(entry);
                if(majority.isEmpty()) {
                    unresolvedSuffix=true; // permanent Phase 7 barrier, even if late ACK appears
                    return new MutationResult(Code.NO_QUORUM,0);
                }
                commit.commit(entry,majority.orElseThrow()); // durable force + commit snapshot publication
                long applied=applier.applyThrough(next,wal.durableCommitIndex(),log);
                if(applied<next)throw new IllegalStateException("committed entry not applied");
                publishAppliedIndex.accept(applied);
                return new MutationResult(Code.OK,next);
            } catch(IOException failure) {
                latchFatal(failure);throw failure;
            } catch(RuntimeException | Error unexpected) {
                IOException failure=new IOException(localEntryDurable
                        ? "failure after durable leader ENTRY; unsafe to accept further writes"
                        : "leader mutation pipeline failed; uncertain WAL state",unexpected);
                latchFatal(failure);
                if(unexpected instanceof Error fatalError)throw fatalError;
                throw failure;
            }
        }
    }

    private void latchFatal(IOException failure) {
        if(storageFault.compareAndSet(null,failure)) {
            unresolvedSuffix=true;
            gate.closeAdmission();
            recordFault.accept(failure); // never acquire NodeServer lifecycle monitor here
        }
    }
}
