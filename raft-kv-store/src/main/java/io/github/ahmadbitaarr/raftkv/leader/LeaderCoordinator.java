package io.github.ahmadbitaarr.raftkv.leader;

import io.github.ahmadbitaarr.raftkv.commit.AckTracker;
import io.github.ahmadbitaarr.raftkv.commit.CommitManager;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.replication.*;
import io.github.ahmadbitaarr.raftkv.recovery.RecoveryManager;
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
    public enum Code { OK, NOT_READY, NO_QUORUM, REQUEST_ID_CONFLICT }
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
    private final RequestDeduplicator deduplicator=new RequestDeduplicator();
    private final AtomicReference<IOException> storageFault=new AtomicReference<>();
    private final boolean freshAtStartup;
    private volatile boolean unresolvedSuffix;
    private volatile Runnable committedWakeup=()->{};

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
        try { deduplicator.reconstruct(log,wal.durableCommitIndex()); }
        catch(IOException invalidCommittedHistory) {
            throw new IllegalStateException("committed request reconstruction failed closed",invalidCommittedHistory);
        }
    }

    /** Called only for the original empty-WAL startup, not after tail canonicalization. */
    public void openAfterStartup() {if(freshAtStartup)gate.open();}

    /** Recovered nonempty startup: no arbitrary boolean or unverified high-water mark can open writes. */
    public void openAfterVerifiedRecovery(RecoveryManager.Certificate certificate) {
        if(certificate==null || !certificate.validFor(log,wal,applier) || unresolvedSuffix
                || storageFault.get()!=null || gate.isActive())
            throw new IllegalStateException("invalid recovered-leader write authorization");
        gate.open();
    }
    public void closeAdmission() {gate.closeAdmission();deduplicator.closeAdmission();}
    RequestDeduplicator deduplicatorForTest(){return deduplicator;}
    public boolean hasUnresolvedSuffix(){return unresolvedSuffix;}
    public IOException storageFailure(){return storageFault.get();}
    /** A proven recovered-cluster authority violation also closes an active leader writer. */
    public void failExternally(IOException failure) {
        Objects.requireNonNull(failure);
        if(storageFault.compareAndSet(null,failure)) {
            unresolvedSuffix=true;
            closeAdmission();
        }
    }
    private void requireNoExternalFailure() throws IOException {
        IOException failure=storageFault.get();
        if(failure!=null)throw new IOException("leader admission invalidated by fatal storage/authority failure",failure);
    }
    public boolean isActive(){return gate.isActive();}
    public boolean awaitDrained(long timeout, TimeUnit unit) throws InterruptedException {
        return gate.awaitDrained(timeout,unit);
    }

    /** A catch-up wake-up is advisory and cannot affect an already finalized client mutation. */
    public void setCommitWakeup(Runnable listener){committedWakeup=Objects.requireNonNull(listener);}

    /** Classify before acquiring the single-writer permit so identical callers JOIN instead of racing it. */
    public MutationResult mutate(String requestId, Command command) throws IOException {
        Objects.requireNonNull(requestId,"requestId");Objects.requireNonNull(command,"command");
        if(requestId.isBlank())throw new IllegalArgumentException("blank request ID");
        if(!gate.admissionOpen() || storageFault.get()!=null)
            return new MutationResult(Code.NOT_READY,0);
        byte[] fingerprint=CanonicalCommandFingerprint.of(command);
        var decision=deduplicator.classify(requestId,command,fingerprint);
        switch(decision.kind()) {
            case COMMITTED_DUPLICATE -> {
                if(!gate.admissionOpen() || storageFault.get()!=null)
                    return new MutationResult(Code.NOT_READY,0);
                return new MutationResult(Code.OK,decision.commitIndex());
            }
            case CONFLICT -> {return new MutationResult(Code.REQUEST_ID_CONFLICT,0);}
            case JOIN -> {
                var completed=RequestDeduplicator.await(decision.completion(),config.clientDeadlineMs());
                if(!gate.admissionOpen() || storageFault.get()!=null)
                    return new MutationResult(Code.NOT_READY,0);
                return new MutationResult(switch(completed.code()) {
                    case OK -> Code.OK;
                    case NO_QUORUM -> Code.NO_QUORUM;
                    case NOT_READY -> Code.NOT_READY;
                },completed.commitIndex());
            }
            case CLOSED, UNRESOLVED -> {return new MutationResult(Code.NOT_READY,0);}
            case NEW -> { /* sole owner continues below */ }
        }
        try {
            MutationResult result=mutateOwned(requestId,command);
            var visible=deduplicator.finish(decision.owner(),new RequestDeduplicator.Completion(
                    switch(result.code()) {
                        case OK -> RequestDeduplicator.CompletionCode.OK;
                        case NO_QUORUM -> RequestDeduplicator.CompletionCode.NO_QUORUM;
                        default -> RequestDeduplicator.CompletionCode.NOT_READY;
                    },result.commitIndex()),result.code()==Code.NO_QUORUM || unresolvedSuffix);
            return new MutationResult(switch(visible.code()) {
                case OK -> Code.OK;
                case NO_QUORUM -> Code.NO_QUORUM;
                case NOT_READY -> Code.NOT_READY;
            },visible.commitIndex());
        }catch(IOException | RuntimeException | Error failure) {
            deduplicator.finish(decision.owner(),
                    new RequestDeduplicator.Completion(RequestDeduplicator.CompletionCode.NOT_READY,0),
                    unresolvedSuffix);
            throw failure;
        }
    }

    private MutationResult mutateOwned(String requestId,Command command) throws IOException {
        var permit=gate.tryAcquire();
        if(permit==null) return new MutationResult(Code.NOT_READY,0);
        try(permit) {
            if(storageFault.get()!=null || unresolvedSuffix)
                return new MutationResult(Code.NOT_READY,0);
            // Bound fresh-prefix probes independently of any pre-append reservation wait.
            // Local catch-up slot contention must not consume the post-ENTRY ACK budget.
            long probeDeadline=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(
                    2L*config.statusDeadlineMs());
            // Probe only previously unknown followers. Nonempty/unavailable history NEVER implies prefix zero.
            for(String follower:config.members().keySet()) {
                if(System.nanoTime()>=probeDeadline)break;
                if(follower.equals(config.configuredLeaderId()) || tracker.verifiedMatchingThrough(follower)>=0)continue;
                try {probe.probeStatus(follower).ifPresent(status -> tracker.certifyEmpty(follower,status));}
                catch(RuntimeException peerUnavailable) { /* unknown is not a quorum certificate */ }
            }
            ReplicationManager.LiveReservation liveReservation;
            try {
                long committedPrefix=wal.durableCommitIndex();
                liveReservation=replication.reserveLivePriority(config.replicationDeadlineMs()+250L,
                        id -> tracker.verifiedMatchingThrough(id)>=committedPrefix);
            } catch(InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return new MutationResult(Code.NOT_READY,0);
            }
            if(liveReservation==null)return new MutationResult(Code.NOT_READY,0);
            try(liveReservation) {
            // Fresh bounded replication decision starts only after the pre-append
            // reservation is obtained. A late ACK cannot change this deadline.
            long networkDeadline=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(
                    config.replicationDeadlineMs()+250L);
            boolean localEntryDurable=false;
            try {
                long previous=log.lastLogIndex();
                if(previous==Long.MAX_VALUE || wal.lastDurableLogIndex()!=previous
                        || wal.durableCommitIndex()!=previous)
                    throw new IOException("leader has unresolved or inconsistent WAL prefix");
                long next=previous+1;
                // Record before WAL I/O so even an uncertain persisted outcome cannot be retried as new.
                LogEntry entry=new LogEntry(next,requestId,command,CanonicalCommandFingerprint.of(command));
                log.append(entry); // leader ENTRY force(true)
                localEntryDurable=true;
                publishDurableIndex.accept(next);

                // One global budget for both follower attempts; slow peer cannot obstruct fast peer.
                ReplicationManager.BatchResult outcomes=replication.replicateStoredEntryForLiveWrite(next,
                        networkDeadline,id -> tracker.verifiedMatchingThrough(id)>=next-1);
                for(var accepted:outcomes.outcomes().entrySet()) {
                    tracker.recordDurableAck(accepted.getKey(),entry,accepted.getValue());
                }
                requireNoExternalFailure();
                Optional<AckTracker.MajorityCertificate> majority=tracker.majorityFor(entry);
                if(majority.isEmpty()) {
                    unresolvedSuffix=true; // permanent Phase 7 barrier, even if late ACK appears
                    return new MutationResult(Code.NO_QUORUM,0);
                }
                requireNoExternalFailure();
                commit.commit(entry,majority.orElseThrow()); // durable force + commit snapshot publication
                long applied=applier.applyThrough(next,wal.durableCommitIndex(),log);
                if(applied<next)throw new IllegalStateException("committed entry not applied");
                publishAppliedIndex.accept(applied);
                requireNoExternalFailure(); // never return OK after a confirmed concurrent authority fault
                try { committedWakeup.run(); } catch(RuntimeException ignored) { /* advisory only */ }
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
            } // live replication priority reservation
        }
    }

    private void latchFatal(IOException failure) {
        if(storageFault.compareAndSet(null,failure)) {
            unresolvedSuffix=true;
            closeAdmission();
            recordFault.accept(failure); // never acquire NodeServer lifecycle monitor here
        }
    }
}
