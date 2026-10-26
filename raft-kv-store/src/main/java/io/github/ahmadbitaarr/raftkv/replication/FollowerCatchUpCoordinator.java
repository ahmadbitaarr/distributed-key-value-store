package io.github.ahmadbitaarr.raftkv.replication;

import io.github.ahmadbitaarr.raftkv.commit.AckTracker;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.log.WalBackedPersistentLog;
import io.github.ahmadbitaarr.raftkv.server.NodeRole;
import io.github.ahmadbitaarr.raftkv.server.NodeStatusSnapshot;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/** Bounded periodic follower repair. No leader COMMIT, suffix-barrier changes, or quorum decisions. */
public final class FollowerCatchUpCoordinator implements AutoCloseable {
    private static final int MAX_ENTRIES_PER_SESSION=24;
    private final ClusterConfig config;
    private final WalBackedPersistentLog log;
    private final Supplier<NodeStatusSnapshot> leaderStatus;
    private final ReplicationManager manager;
    private final FollowerMaintenanceClient client;
    private final AckTracker tracker;
    private final BooleanSupplier unresolvedLeaderSuffix;
    private final ScheduledThreadPoolExecutor scheduler;
    private final Map<String,AtomicBoolean> scheduled=new HashMap<>();
    private final ConcurrentMap<String,String> diagnostics=new ConcurrentHashMap<>();
    private final Set<String> unsafe=ConcurrentHashMap.newKeySet();
    private final Set<String> disconnected=ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closing=new AtomicBoolean();
    private final AtomicBoolean started=new AtomicBoolean();

    public FollowerCatchUpCoordinator(ClusterConfig config,WalBackedPersistentLog log,
            Supplier<NodeStatusSnapshot> leaderStatus,
            ReplicationManager manager,FollowerMaintenanceClient client,AckTracker tracker,
            BooleanSupplier unresolvedLeaderSuffix) {
        this.config=Objects.requireNonNull(config);this.log=Objects.requireNonNull(log);
        this.leaderStatus=Objects.requireNonNull(leaderStatus);this.manager=Objects.requireNonNull(manager);
        this.client=Objects.requireNonNull(client);this.tracker=Objects.requireNonNull(tracker);
        this.unresolvedLeaderSuffix=Objects.requireNonNull(unresolvedLeaderSuffix);
        if(config.localRole()!=NodeRole.LEADER)throw new IllegalArgumentException("leader catch-up only");
        for(String id:config.members().keySet())if(!id.equals(config.configuredLeaderId())) {
            scheduled.put(id,new AtomicBoolean());diagnostics.put(id,"UNKNOWN");
        }
        AtomicInteger sequence=new AtomicInteger();
        scheduler=new ScheduledThreadPoolExecutor(scheduled.size(),r -> {
            Thread thread=new Thread(r,"raftkv-catch-up-"+sequence.incrementAndGet());
            thread.setDaemon(true);return thread;
        });
        scheduler.setRemoveOnCancelPolicy(true);
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    }

    public void start(){
        if(!started.compareAndSet(false,true))throw new IllegalStateException("catch-up started twice");
        for(String id:scheduled.keySet())
            scheduler.scheduleWithFixedDelay(()->scheduleSession(id),0,500,TimeUnit.MILLISECONDS);
    }
    /** Advisory wake after COMMIT; coalesced with any already scheduled/running session. */
    public void wake(){if(!closing.get() && started.get())for(String id:scheduled.keySet())scheduleSession(id);}
    public Map<String,String> diagnosticSnapshot(){return Map.copyOf(diagnostics);}
    public void closeAdmission(){closing.set(true);}
    private void scheduleSession(String id){
        if(closing.get() || unsafe.contains(id))return;
        AtomicBoolean once=scheduled.get(id);
        if(!once.compareAndSet(false,true))return;
        try {scheduler.execute(()->{
            try {if(!closing.get())runSession(id);}
            catch(RuntimeException failure){diagnostics.put(id,"RETRY: "+failure.getClass().getSimpleName());}
            finally{once.set(false);}
        });}
        catch(RejectedExecutionException stopped){once.set(false);}
    }

    /** Package-independent test hook; runs a single bounded session without scheduling retries. */
    public void runOnce(String follower){
        AtomicBoolean once=scheduled.get(follower);
        if(once==null)throw new IllegalArgumentException("unknown follower");
        if(closing.get() || !once.compareAndSet(false,true))return;
        try {runSession(follower);} finally {once.set(false);}
    }

    private void markUnsafe(String follower,String reason){
        unsafe.add(follower);tracker.invalidate(follower);
        diagnostics.put(follower,"UNSAFE: "+reason);
    }

    private void runSession(String follower){
        if(closing.get() || unsafe.contains(follower))return;
        // Status, replication, commit and truncation all use the SAME per-peer active slot.
        Optional<Optional<FollowerStatusProbe.Status>> read=manager.backgroundAttempt(follower,
                ()->client.probeStatus(follower));
        if(read.isEmpty())return; // live RPC takes priority or previous worker still executing
        if(closing.get())return;
        Optional<FollowerStatusProbe.Status> maybe=read.get();
        if(maybe.isEmpty()) {
            disconnected.add(follower);
            diagnostics.put(follower,"UNAVAILABLE");return;
        }
        var status=maybe.get();
        if(!follower.equals(status.nodeId()) || status.role()!=NodeRole.FOLLOWER
                || status.appliedIndex()>status.commitIndex() || status.commitIndex()>status.lastLogIndex()) {
            markUnsafe(follower,"invalid follower status");return;
        }
        // Sample AFTER the potentially slow follower status RPC. This acquire-read observes
        // NodeServer's atomic publication, not WalStore's nonvolatile in-memory metadata.
        // All safety decisions in this session use the SAME authoritative commit boundary;
        // newly published commits are picked up by the next bounded session.
        NodeStatusSnapshot authoritative=Objects.requireNonNull(leaderStatus.get(),"leader status");
        if(authoritative.role()!=NodeRole.LEADER
                || !config.localNodeId().equals(authoritative.localNodeId())) {
            diagnostics.put(follower,"INVALID LEADER SNAPSHOT");return;
        }
        long leaderCommit=authoritative.durableCommitIndex();
        if(status.commitIndex()>leaderCommit) {
            // A concurrent leader COMMIT publication can overtake our first read.
            // Refresh the ENTIRE snapshot before permanently classifying the peer unsafe.
            NodeStatusSnapshot refreshed=Objects.requireNonNull(leaderStatus.get(),"leader status");
            if(refreshed.role()!=NodeRole.LEADER
                    || !config.localNodeId().equals(refreshed.localNodeId())) {
                diagnostics.put(follower,"INVALID LEADER SNAPSHOT");return;
            }
            authoritative=refreshed;
            leaderCommit=authoritative.durableCommitIndex();
            if(status.commitIndex()>leaderCommit) {
                markUnsafe(follower,"follower committed beyond leader");return;
            }
        }
        long known=tracker.verifiedMatchingThrough(follower);
        if(disconnected.remove(follower) || known>status.lastLogIndex()) {
            tracker.invalidate(follower);known=-1;
        }
        if(known<0) {
            boolean initialized=status.lastLogIndex()==0
                    ? tracker.certifyEmpty(follower,status)
                    : tracker.beginNonemptyVerification(follower,status);
            if(!initialized){diagnostics.put(follower,"UNVERIFIED");return;}
            known=0;
        }
        long target=leaderCommit; // NEVER include an unresolved leader suffix
        int transferred=0;
        while(!closing.get() && known<target && transferred<MAX_ENTRIES_PER_SESSION) {
            long end=Math.min(target,known+Math.min(8,MAX_ENTRIES_PER_SESSION-transferred));
            List<LogEntry> slice=log.snapshotRange(known+1,end,8);
            for(LogEntry entry:slice) {
                if(closing.get())return;
                var maybeOutcome=manager.backgroundAttempt(follower,()->client.replicate(follower,entry));
                if(maybeOutcome.isEmpty())return;
                FollowerClient.Outcome outcome=maybeOutcome.get();
                if(outcome.kind()==FollowerClient.Kind.ACK) {
                    if(!tracker.recordDurableAck(follower,entry,outcome)) {
                        diagnostics.put(follower,"UNVERIFIED ACK");return;
                    }
                    known=entry.index();transferred++;
                    continue;
                }
                if(outcome.kind()==FollowerClient.Kind.INDEX_CONFLICT) {
                    if(entry.index()<=status.commitIndex() || known<status.commitIndex()) {
                        markUnsafe(follower,"committed-prefix conflict at "+entry.index());return;
                    }
                    // Only the verified retained prefix may survive; never truncate leader history.
                    if(known!=entry.index()-1 || known<status.appliedIndex()
                            || unresolvedLeaderSuffix.getAsBoolean()) {
                        markUnsafe(follower,"unverified unsafe truncation");return;
                    }
                    long retained=known;
                    if(closing.get())return;
                    var rewrite=manager.backgroundAttempt(follower,
                            ()->client.truncateUncommittedSuffix(follower,retained));
                    if(rewrite.isEmpty() || !rewrite.get().accepted())return;
                    // A successful rewrite invalidates prior certificates; reverify from zero next session.
                    tracker.invalidate(follower);
                    diagnostics.put(follower,"RECONCILED; REVERIFY");return;
                }
                if(outcome.kind()==FollowerClient.Kind.INDEX_GAP)
                    diagnostics.put(follower,"INDEX_GAP; RETRY");
                else diagnostics.put(follower,outcome.kind().name());
                return;
            }
        }
        long verified=tracker.verifiedMatchingThrough(follower);
        long toCommit=Math.min(leaderCommit,verified);
        if(closing.get())return;
        if(toCommit>status.commitIndex() || (toCommit>0 && status.appliedIndex()<toCommit)) {
            var result=manager.backgroundAttempt(follower,()->client.advanceCommit(follower,toCommit));
            if(result.isEmpty() || !result.get().accepted()) {
                diagnostics.put(follower,"COMMIT RETRY");return;
            }
        }
        diagnostics.put(follower,verified>=leaderCommit?"SYNCHRONIZED":"BEHIND");
    }

    @Override public void close() throws IOException {
        closeAdmission();scheduler.shutdown();
        try {
            if(!scheduler.awaitTermination(config.shutdownGraceMs(),TimeUnit.MILLISECONDS)) {
                scheduler.shutdownNow();
                if(!scheduler.awaitTermination(config.shutdownGraceMs(),TimeUnit.MILLISECONDS))
                    throw new IOException("catch-up worker still active; WAL lease must remain held");
            }
        }catch(InterruptedException interrupted) {
            Thread.currentThread().interrupt();scheduler.shutdownNow();
            throw new IOException("interrupted draining catch-up work; WAL lease retained",interrupted);
        }
    }
}
