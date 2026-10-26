package io.github.ahmadbitaarr.raftkv.replication;

import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.log.WalBackedPersistentLog;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.LongSupplier;

/** Explicit delivery of already-durable entries; never commits, counts quorum or retries. */
public final class ReplicationManager implements AutoCloseable {
    private final ClusterConfig config;
    private final WalBackedPersistentLog leaderLog;
    private final FollowerClient client;
    private final long waitMillis;
    private final LongSupplier nanoClock;
    private final ExecutorService workers;
    private final Map<String, Peer> peers = new ConcurrentHashMap<>();
    private volatile boolean closed;
    private final Object arbitration=new Object();
    private boolean livePriority;

    /** Reserve priority BEFORE leader ENTRY force, so background contention fails as NOT_READY. */
    public LiveReservation reserveLivePriority(long maxWaitMillis) throws InterruptedException {
        return reserveLivePriority(maxWaitMillis,id->true);
    }

    /** Defer before local append if background owns the only verified matching-prefix peer. */
    public LiveReservation reserveLivePriority(long maxWaitMillis,java.util.function.Predicate<String> certifiedPeer)
            throws InterruptedException {
        Objects.requireNonNull(certifiedPeer);
        if(maxWaitMillis<0)throw new IllegalArgumentException("negative reservation wait");
        synchronized(arbitration) {
            if(closed || livePriority)return null;
            livePriority=true; // atomic with every background slot acquisition
            long deadline=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(maxWaitMillis);
            // A free peer can support quorum even while the other has a slow catch-up RPC.
            while(!closed && (peers.values().stream().allMatch(peer -> peer.active.get())
                    || (peers.keySet().stream().anyMatch(certifiedPeer)
                        && peers.entrySet().stream().noneMatch(e -> certifiedPeer.test(e.getKey())
                                && !e.getValue().active.get())))) {
                long remaining=deadline-System.nanoTime();
                if(remaining<=0) {livePriority=false;arbitration.notifyAll();return null;}
                TimeUnit.NANOSECONDS.timedWait(arbitration,remaining);
            }
            if(closed){livePriority=false;arbitration.notifyAll();return null;}
            return new LiveReservation();
        }
    }
    public final class LiveReservation implements AutoCloseable {
        private boolean released;
        @Override public void close(){synchronized(arbitration){if(!released){released=true;livePriority=false;arbitration.notifyAll();}}}
    }

    /** One background RPC at a time per follower, yielding at every RPC boundary. */
    public <T> Optional<T> backgroundAttempt(String follower, Callable<T> operation) {
        Objects.requireNonNull(operation);
        Peer peer=requirePeer(follower);
        synchronized(arbitration) {
            if(closed || livePriority || !peer.active.compareAndSet(false,true))return Optional.empty();
        }
        try {
            return Optional.ofNullable(operation.call());
        }catch(Exception failure){return Optional.empty();}
        finally {
            synchronized(arbitration){peer.active.set(false);arbitration.notifyAll();}
        }
    }


    public record BatchResult(Map<String,FollowerClient.Outcome> outcomes) {
        public BatchResult { outcomes=Map.copyOf(outcomes); }
    }
    private static final class Peer {
        final AtomicBoolean active = new AtomicBoolean();
        final AtomicLong acceptedRequestIndex = new AtomicLong();
        final AtomicLong reportedPersistedIndex = new AtomicLong();
    }

    public ReplicationManager(ClusterConfig config, WalBackedPersistentLog leaderLog, FollowerClient client) {
        this(config,leaderLog,client,config.replicationDeadlineMs()+250L);
    }
    /** Test/harness attempt budget; the RPC also carries its own bounded deadline. */
    public ReplicationManager(ClusterConfig config, WalBackedPersistentLog leaderLog,
            FollowerClient client, long waitMillis) {
        this(config,leaderLog,client,waitMillis,System::nanoTime);
    }
    /** Package-private monotonic clock seam for deterministic deadline-race tests. */
    ReplicationManager(ClusterConfig config, WalBackedPersistentLog leaderLog,
            FollowerClient client, long waitMillis, LongSupplier nanoClock) {
        this.nanoClock=Objects.requireNonNull(nanoClock,"nanoClock");
        this.config=Objects.requireNonNull(config);
        this.leaderLog=Objects.requireNonNull(leaderLog);
        this.client=Objects.requireNonNull(client);
        if(config.localRole()!=io.github.ahmadbitaarr.raftkv.server.NodeRole.LEADER)
            throw new IllegalArgumentException("replication manager must be configured leader");
        if(waitMillis<1 || waitMillis>60_250) throw new IllegalArgumentException("invalid attempt budget");
        this.waitMillis=waitMillis;
        config.members().keySet().stream().filter(id->!id.equals(config.configuredLeaderId()))
                .forEach(id->peers.put(id,new Peer()));
        AtomicInteger threadCounter=new AtomicInteger();
        workers=Executors.newFixedThreadPool(peers.size(),task->{
            Thread t=new Thread(task,"raftkv-replication-"+threadCounter.incrementAndGet());
            t.setDaemon(true); return t;
        });
    }

    private record Submission(FollowerClient.Outcome immediate, Future<FollowerClient.Outcome> future) { }

    private LogEntry requireStoredEntry(long index) {
        if(index<=0) throw new IllegalArgumentException("entry index must be positive");
        // Caller passes a previously WAL-forced leader entry; Phase 6 does not mutate it.
        return leaderLog.get(index).orElseThrow(()->new IllegalArgumentException("leader entry not present"));
    }

    private Submission submit(String followerId, LogEntry entry) {
        return submit(followerId, entry, null);
    }

    private record Completion(String followerId, FollowerClient.Outcome outcome) { }

    private Submission submit(String followerId, LogEntry entry,
            java.util.function.Consumer<Completion> completed) {
        long index=entry.index();
        Peer peer=peers.get(followerId);
        if(peer==null) throw new IllegalArgumentException("unknown follower");
        if(closed) return new Submission(new FollowerClient.Outcome(FollowerClient.Kind.REMOTE_ERROR,index,0,0,"manager closed"),null);
        if(!peer.active.compareAndSet(false,true))
            return new Submission(new FollowerClient.Outcome(FollowerClient.Kind.BUSY,index,0,0,"follower attempt active"),null);
        try {
            Future<FollowerClient.Outcome> future=workers.submit(()->{
                FollowerClient.Outcome result;
                try {
                    result=client.replicate(followerId,entry);
                    if(result==null || result.requestedIndex()!=index || result.kind()==null)
                        result=new FollowerClient.Outcome(FollowerClient.Kind.INVALID_RESPONSE,index,0,0,"bad client outcome");
                    if(result.kind()==FollowerClient.Kind.ACK) {
                        if(result.lastReportedPersistedIndex()<index)
                            result=new FollowerClient.Outcome(FollowerClient.Kind.INVALID_RESPONSE,index,0,0,"ACK does not cover entry");
                        else {
                            peer.acceptedRequestIndex.accumulateAndGet(index,Math::max);
                            peer.reportedPersistedIndex.accumulateAndGet(result.lastReportedPersistedIndex(),Math::max);
                        }
                    }
                } catch(RuntimeException e) {
                    result=new FollowerClient.Outcome(FollowerClient.Kind.REMOTE_ERROR,index,0,0,
                            "worker exception: "+e.getClass().getSimpleName());
                } finally {
                    synchronized(arbitration) {
                        peer.active.set(false); // only actual worker exit releases ownership
                        arbitration.notifyAll();
                    }
                }
                if(completed!=null) completed.accept(new Completion(followerId,result));
                return result;
            });
            return new Submission(null,future);
        } catch (RejectedExecutionException rejected) {
            synchronized(arbitration){peer.active.set(false);arbitration.notifyAll();}
            return new Submission(new FollowerClient.Outcome(FollowerClient.Kind.REMOTE_ERROR,index,0,0,
                    "executor rejected attempt"),null);
        }
    }

    private FollowerClient.Outcome await(Submission submission,long index) {
        if(submission.immediate()!=null) return submission.immediate();
        try {
            return submission.future().get(waitMillis,TimeUnit.MILLISECONDS);
        } catch(TimeoutException timedOut) {
            // No cancel: even a cancelled/done future does NOT prove the client worker stopped.
            return new FollowerClient.Outcome(FollowerClient.Kind.UNKNOWN_TIMEOUT,index,0,0,"worker completion unobserved");
        } catch(InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new FollowerClient.Outcome(FollowerClient.Kind.UNKNOWN_TIMEOUT,index,0,0,"manager wait interrupted");
        } catch(ExecutionException error) {
            return new FollowerClient.Outcome(FollowerClient.Kind.REMOTE_ERROR,index,0,0,"worker failed");
        }
    }

    public FollowerClient.Outcome replicateStoredEntry(long index,String followerId) {
        LogEntry entry=requireStoredEntry(index);
        return await(submit(followerId,entry),index);
    }

    /** Dispatch to both followers *before* waiting on either, so one slow peer cannot delay the other. */
    public BatchResult replicateStoredEntry(long index) {
        LogEntry entry=requireStoredEntry(index);
        Map<String,Submission> pending=new LinkedHashMap<>();
        for(String id:new TreeSet<>(peers.keySet())) pending.put(id,submit(id,entry));
        Map<String,FollowerClient.Outcome> answers=new LinkedHashMap<>();
        pending.forEach((id,submission)->answers.put(id,await(submission,index)));
        return new BatchResult(answers);
    }

    /**
     * Phase 7 bounded fanout using a SINGLE decision deadline. Completions are consumed
     * in finish order so a slow follower cannot hide an earlier healthy ACK.
     * Late completions update Phase 6 diagnostic progress only, never this batch result.
     */
    public BatchResult replicateStoredEntryUntil(long index, long deadlineNanos) {
        return replicateUntil(index,deadlineNanos,id->false,false);
    }

    /** Live requests can retry a locally BUSY background slot before the SAME decision deadline.
     * A timely ACK from a previously certified prefix is already a sufficient follower vote. */
    public BatchResult replicateStoredEntryForLiveWrite(long index,long deadlineNanos,
            java.util.function.Predicate<String> certifiedPrefix) {
        return replicateUntil(index,deadlineNanos,Objects.requireNonNull(certifiedPrefix),true);
    }

    private BatchResult replicateUntil(long index,long deadlineNanos,
            java.util.function.Predicate<String> certifiedPrefix,boolean retryBackgroundBusy) {
        LogEntry entry=requireStoredEntry(index);
        BlockingQueue<Completion> completed=new LinkedBlockingQueue<>();
        Map<String,FollowerClient.Outcome> results=new LinkedHashMap<>();
        Set<String> pending=new TreeSet<>();
        Set<String> retryBusy=new TreeSet<>();
        for(String id:new TreeSet<>(peers.keySet())) {
            Submission submission=submit(id,entry,completed::offer);
            if(submission.immediate()!=null) {
                if(retryBackgroundBusy && submission.immediate().kind()==FollowerClient.Kind.BUSY)
                    retryBusy.add(id);
                else results.put(id,submission.immediate());
            } else pending.add(id);
        }
        while(!pending.isEmpty() || !retryBusy.isEmpty()) {
            long remaining=deadlineNanos-nanoClock.getAsLong();
            if(remaining<=0)break;
            for(var it=retryBusy.iterator();it.hasNext();) {
                String id=it.next();
                if(peers.get(id).active.get())continue;
                Submission submission=submit(id,entry,completed::offer);
                if(submission.immediate()==null){pending.add(id);it.remove();}
                else if(submission.immediate().kind()!=FollowerClient.Kind.BUSY){
                    results.put(id,submission.immediate());it.remove();
                }
            }
            if(pending.isEmpty() && retryBusy.isEmpty())break;
            Completion done;
            try {
                long boundedPoll=retryBusy.isEmpty()?remaining:
                        Math.min(remaining,TimeUnit.MILLISECONDS.toNanos(25));
                done=completed.poll(boundedPoll,TimeUnit.NANOSECONDS);
            } catch(InterruptedException interrupted) {
                Thread.currentThread().interrupt();break;
            }
            if(done==null){if(retryBusy.isEmpty())break;else continue;}
            // A completion dequeued at or after the deadline never supplies proof.
            long observedAtNanos=nanoClock.getAsLong();
            if(pending.remove(done.followerId())) {
                var answer=observedAtNanos>=deadlineNanos?decisionTimeout(index):done.outcome();
                results.put(done.followerId(),answer);
                if(retryBackgroundBusy && answer.kind()==FollowerClient.Kind.ACK
                        && certifiedPrefix.test(done.followerId()))break;
            }
            if(observedAtNanos>=deadlineNanos)break;
        }
        for(String id:pending)results.put(id,decisionTimeout(index));
        for(String id:retryBusy)results.put(id,decisionTimeout(index));
        return new BatchResult(results);
    }

    private static FollowerClient.Outcome decisionTimeout(long index) {
        return new FollowerClient.Outcome(FollowerClient.Kind.UNKNOWN_TIMEOUT,index,0,0,
                "no confirmed outcome before coordinator decision deadline");
    }

    public long highestAcceptedRequestIndex(String id) { return requirePeer(id).acceptedRequestIndex.get(); }
    public long lastReportedPersistedIndex(String id) { return requirePeer(id).reportedPersistedIndex.get(); }
    public boolean isActive(String id) { return requirePeer(id).active.get(); }
    private Peer requirePeer(String id) {
        Peer p=peers.get(id); if(p==null) throw new IllegalArgumentException("unknown follower");return p;
    }

    @Override public synchronized void close() throws IOException {
        synchronized(arbitration){closed=true;livePriority=false;arbitration.notifyAll();}
        workers.shutdown(); // Never assume cancel(true) means worker exit.
        try {
            boolean terminated=workers.awaitTermination(config.shutdownGraceMs(),TimeUnit.MILLISECONDS);
            if(!terminated || peers.values().stream().anyMatch(p->p.active.get()))
                throw new IOException("replication workers still active; client ownership retained");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted awaiting replication worker exit",interrupted);
        }
        client.close(); // only after all attempt workers have exited
    }
}
