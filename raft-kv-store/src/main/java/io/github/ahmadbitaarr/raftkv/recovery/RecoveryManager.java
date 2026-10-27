package io.github.ahmadbitaarr.raftkv.recovery;

import io.github.ahmadbitaarr.raftkv.commit.AckTracker;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.log.WalBackedPersistentLog;
import io.github.ahmadbitaarr.raftkv.replication.*;
import io.github.ahmadbitaarr.raftkv.server.NodeRole;
import io.github.ahmadbitaarr.raftkv.statemachine.StateMachineApplier;
import io.github.ahmadbitaarr.raftkv.wal.WalRecoveryResult;
import io.github.ahmadbitaarr.raftkv.wal.WalStore;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;

/** Phase 10 recovered-leader protocol. Owns no WAL or transport channels. */
public final class RecoveryManager implements AutoCloseable {
    public enum State { LOCAL_RECOVERY, AWAITING_FOLLOWER, RECONCILING, VERIFIED, FAILED, CLOSED }
    public record LocalState(long committedIndex, long appliedIndex) {
        public LocalState {
            if (committedIndex < 0 || appliedIndex != committedIndex)
                throw new IllegalArgumentException("incomplete local replay");
        }
    }
    /** Opaque, process/owner-bound authorization: only this manager can construct one. */
    public static final class Certificate {
        private final RecoveryManager issuer;
        private final WalBackedPersistentLog owner;
        private final WalStore durableOwner;
        private final String follower;
        private final long index;
        private Certificate(RecoveryManager issuer, WalBackedPersistentLog owner, WalStore durableOwner,
                String follower, long index) {
            this.issuer=issuer;this.owner=owner;this.durableOwner=durableOwner;
            this.follower=follower;this.index=index;
        }
        public long committedIndex(){return index;}
        public String followerId(){return follower;}
        /** No public constructor or caller-provided quorum boolean can forge this proof. */
        public boolean validFor(WalBackedPersistentLog log, WalStore wal, StateMachineApplier applier) {
            return issuer.issued.get()==this && !issuer.closing.get() && issuer.failure.get()==null
                    && owner==log && durableOwner==wal && index==wal.durableCommitIndex()
                    && index==wal.lastDurableLogIndex() && index==log.lastLogIndex()
                    && index==applier.appliedIndex() && issuer.tracker.verifiedMatchingThrough(follower)>=index;
        }
    }

    /** Called with the one owned WAL before opening network or any admission. */
    public static LocalState recoverLocal(WalBackedPersistentLog log,WalStore wal,
            StateMachineApplier applier) throws IOException {
        Objects.requireNonNull(log);Objects.requireNonNull(wal);Objects.requireNonNull(applier);
        WalRecoveryResult snapshot=wal.recoveryResult();
        long last=snapshot.entries().size();
        long committed=snapshot.commitIndex();
        if (committed<0 || committed>last || wal.lastDurableLogIndex()!=last
                || log.lastLogIndex()!=last || wal.durableCommitIndex()!=committed
                || applier.appliedIndex()!=0)
            throw new IOException("inconsistent recovered leader WAL/application metadata");
        try {
            if (last>committed)log.truncateAfter(committed); // durable rewrite, verified on reopen
            if (log.lastLogIndex()!=committed || wal.lastDurableLogIndex()!=committed
                    || wal.durableCommitIndex()!=committed
                    || wal.recoveryResult().commitIndex()!=committed
                    || wal.recoveryResult().entries().size()!=committed)
                throw new IOException("leader canonical rewrite was not verified");
            long applied=applier.applyThrough(committed,wal.durableCommitIndex(),log);
            if(applied!=committed)throw new IOException("leader replay did not reach durable COMMIT");
            return new LocalState(committed,applied);
        }catch(IOException failure){throw failure;}
        catch(RuntimeException | Error failure){throw new IOException("leader committed-prefix replay failed closed",failure);}
    }

    private static final int MAX_ENTRIES_PER_ATTEMPT=24;
    private final ClusterConfig config;
    private final WalBackedPersistentLog log;
    private final WalStore wal;
    private final StateMachineApplier applier;
    private final ReplicationManager transport;
    private final FollowerMaintenanceClient client;
    private final AckTracker tracker;
    private final Consumer<Certificate> onVerified;
    private final Consumer<IOException> onFatal;
    private final ScheduledThreadPoolExecutor executor;
    private final Map<String,AtomicBoolean> running=new HashMap<>();
    private final ConcurrentMap<String,String> diagnostics=new ConcurrentHashMap<>();
    private final AtomicReference<Certificate> issued=new AtomicReference<>();
    private final AtomicReference<IOException> failure=new AtomicReference<>();
    private final AtomicBoolean closing=new AtomicBoolean();
    private final AtomicBoolean started=new AtomicBoolean();
    private volatile State state=State.LOCAL_RECOVERY;

    public RecoveryManager(ClusterConfig config,WalBackedPersistentLog log,WalStore wal,
            StateMachineApplier applier,ReplicationManager transport,FollowerMaintenanceClient client,
            AckTracker tracker,Consumer<Certificate> onVerified,Consumer<IOException> onFatal) {
        this.config=Objects.requireNonNull(config);this.log=Objects.requireNonNull(log);
        this.wal=Objects.requireNonNull(wal);this.applier=Objects.requireNonNull(applier);
        this.transport=Objects.requireNonNull(transport);this.client=Objects.requireNonNull(client);
        this.tracker=Objects.requireNonNull(tracker);
        this.onVerified=Objects.requireNonNull(onVerified);this.onFatal=Objects.requireNonNull(onFatal);
        if(config.localRole()!=NodeRole.LEADER)throw new IllegalArgumentException("leader recovery only");
        if(log.lastLogIndex()!=wal.durableCommitIndex() || wal.lastDurableLogIndex()!=log.lastLogIndex()
                || applier.appliedIndex()!=wal.durableCommitIndex())
            throw new IllegalArgumentException("local committed state must be canonical before reconciliation");
        for(String id:config.members().keySet())if(!id.equals(config.configuredLeaderId())) {
            running.put(id,new AtomicBoolean());diagnostics.put(id,"UNKNOWN");
            tracker.invalidate(id); // never inherit Phase 7/9 stale process-local proofs
        }
        AtomicInteger seq=new AtomicInteger();
        // One bounded reconciliation worker at a time. At witness publication there
        // can be no second recovery session with stale tail decisions still running.
        // Both peers remain independently retried by their periodic tasks.
        executor=new ScheduledThreadPoolExecutor(1,r->{
            Thread t=new Thread(r,"raftkv-recovery-"+seq.incrementAndGet());t.setDaemon(true);return t;
        });
        executor.setRemoveOnCancelPolicy(true);
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        state=State.AWAITING_FOLLOWER;
    }
    public State state(){return state;}
    public IOException failure(){return failure.get();}
    public Map<String,String> diagnostics(){return Map.copyOf(diagnostics);}
    public boolean accepts(Certificate cert){return cert!=null && issued.get()==cert && !closing.get()
            && failure.get()==null && cert.validFor(log,wal,applier);}
    public void start(){
        if(closing.get() || !started.compareAndSet(false,true))throw new IllegalStateException("recovery not startable");
        for(String id:running.keySet())
            executor.scheduleWithFixedDelay(()->schedule(id),0,500,TimeUnit.MILLISECONDS);
    }
    public void closeAdmission(){closing.set(true);}
    private void schedule(String follower) {
        if(closing.get() || issued.get()!=null || failure.get()!=null)return;
        AtomicBoolean busy=running.get(follower);
        if(!busy.compareAndSet(false,true))return;
        try{executor.execute(()->{
            try{attempt(follower);}catch(RuntimeException unexpected){
                diagnostics.put(follower,"RETRY: "+unexpected.getClass().getSimpleName());
            }finally{busy.set(false);}
        });}catch(RejectedExecutionException ignored){busy.set(false);}
    }
    /** Deterministic test seam: one bounded attempt, no periodic scheduling or sleeps. */
    public void runOnce(String follower) {
        AtomicBoolean busy=running.get(follower);
        if(busy==null)throw new IllegalArgumentException("unknown follower");
        if(closing.get() || issued.get()!=null || failure.get()!=null || !busy.compareAndSet(false,true))return;
        try{attempt(follower);}finally{busy.set(false);}
    }
    private <T> Optional<T> rpc(String id,Callable<T> call){
        if(closing.get() || failure.get()!=null)return Optional.empty();
        return transport.backgroundAttempt(id,call); // same per-follower transport slot as Phase 9
    }
    private void fatal(String reason) {
        IOException e=new IOException("unsafe recovered leader follower history: "+reason);
        if(failure.compareAndSet(null,e)) {
            issued.set(null);state=State.FAILED;closeAdmission();
            onFatal.accept(e); // never acquire NodeServer's synchronized lifecycle monitor
        }
    }
    private void attempt(String follower) {
        if(closing.get() || failure.get()!=null || issued.get()!=null)return;
        var outer=rpc(follower,()->client.probeStatus(follower));
        if(outer.isEmpty())return;
        var reported=outer.get();
        if(reported.isEmpty()){diagnostics.put(follower,"UNAVAILABLE");return;}
        var s=reported.orElseThrow();
        if(!follower.equals(s.nodeId()) || s.role()!=NodeRole.FOLLOWER || s.ready()
                || s.appliedIndex()>s.commitIndex() || s.commitIndex()>s.lastLogIndex()) {
            diagnostics.put(follower,"INVALID STATUS");tracker.invalidate(follower);return;
        }
        long c=wal.durableCommitIndex(); // no mutation admission while recovering; immutable canonical COMMIT
        if(s.commitIndex()>c){fatal("committed follower "+follower+" ahead of leader");return;}
        state=State.RECONCILING;
        long known=tracker.verifiedMatchingThrough(follower);
        if(known>s.lastLogIndex()) {tracker.invalidate(follower);known=-1;}
        if(known<0) {
            boolean initial=s.lastLogIndex()==0?tracker.certifyEmpty(follower,s)
                    :tracker.beginNonemptyVerification(follower,s);
            if(!initial){diagnostics.put(follower,"UNVERIFIED");return;}
            known=0;
        }
        int checked=0;
        while(!closing.get() && known<c && checked<MAX_ENTRIES_PER_ATTEMPT) {
            LogEntry entry=log.get(known+1).orElseThrow();
            var call=rpc(follower,()->client.replicate(follower,entry));
            if(call.isEmpty())return;
            FollowerClient.Outcome response=call.get();
            if(response.kind()==FollowerClient.Kind.ACK) {
                if(!tracker.recordDurableAck(follower,entry,response)){
                    diagnostics.put(follower,"INVALID ACK");tracker.invalidate(follower);return;
                }
                known++;checked++;continue;
            }
            if(response.kind()==FollowerClient.Kind.INDEX_CONFLICT) {
                if(entry.index()<=s.commitIndex() || known<s.commitIndex()) {
                    fatal("committed-prefix conflict on "+follower+" at "+entry.index());return;
                }
                if(known!=entry.index()-1 || known<s.appliedIndex()) {
                    diagnostics.put(follower,"UNVERIFIED TRUNCATION");return;
                }
                long retain=known;
                var truncate=rpc(follower,()->client.truncateUncommittedSuffix(follower,retain));
                if(truncate.isEmpty() || !truncate.get().accepted() || truncate.get().lastLogIndex()!=retain) {
                    diagnostics.put(follower,"TRUNCATION RETRY");return;
                }
                tracker.invalidate(follower);diagnostics.put(follower,"TRUNCATED; REVERIFY");return;
            }
            diagnostics.put(follower,"ENTRY RETRY: "+response.kind());return;
        }
        if(closing.get() || known!=c) {diagnostics.put(follower,"VERIFYING "+known+"/"+c);return;}
        // Any extra uncommitted follower suffix must be removed BEFORE a recovery witness exists.
        if(s.lastLogIndex()>c) {
            var truncated=rpc(follower,()->client.truncateUncommittedSuffix(follower,c));
            if(truncated.isEmpty() || !truncated.get().accepted() || truncated.get().lastLogIndex()!=c) {
                diagnostics.put(follower,"TAIL RETRY");return;
            }
            tracker.invalidate(follower);diagnostics.put(follower,"CANONICALIZED; REVERIFY");return;
        }
        if(s.commitIndex()<c || s.appliedIndex()<c) {
            var committed=rpc(follower,()->client.advanceCommit(follower,c));
            if(committed.isEmpty() || !committed.get().accepted() || committed.get().commitIndex()!=c
                    || committed.get().appliedIndex()!=c) {
                diagnostics.put(follower,"COMMIT RETRY");return;
            }
        }
        // Status is fresh and checked, not just inferred from an RPC ACK or cached high-water mark.
        var confirm=rpc(follower,()->client.probeStatus(follower));
        if(confirm.isEmpty() || confirm.get().isEmpty())return;
        var finalStatus=confirm.get().orElseThrow();
        if(!follower.equals(finalStatus.nodeId()) || finalStatus.role()!=NodeRole.FOLLOWER
                || finalStatus.ready() || finalStatus.lastLogIndex()!=c
                || finalStatus.commitIndex()!=c || finalStatus.appliedIndex()!=c) {
            diagnostics.put(follower,"NOT CANONICAL");return;
        }
        if(tracker.verifiedMatchingThrough(follower)<c)return;
        Certificate cert=new Certificate(this,log,wal,follower,c);
        if(!closing.get() && failure.get()==null && issued.compareAndSet(null,cert)) {
            state=State.VERIFIED;
            diagnostics.put(follower,"WITNESS VERIFIED");
            onVerified.accept(cert);
        }
    }
    @Override public void close() throws IOException {
        closeAdmission();executor.shutdown();
        try {
            if(!executor.awaitTermination(config.shutdownGraceMs(),TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
                if(!executor.awaitTermination(config.shutdownGraceMs(),TimeUnit.MILLISECONDS))
                    throw new IOException("recovery workers still active; WAL lease retained");
            }
        } catch(InterruptedException interrupted) {
            Thread.currentThread().interrupt();executor.shutdownNow();
            throw new IOException("interrupted draining recovery worker; WAL lease retained",interrupted);
        }
        state=State.CLOSED;
    }
}
