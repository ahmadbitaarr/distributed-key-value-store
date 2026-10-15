package io.github.ahmadbitaarr.raftkv.replication;

import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.log.WalBackedPersistentLog;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Explicit delivery of already-durable entries; never commits, counts quorum or retries. */
public final class ReplicationManager implements AutoCloseable {
    private final ClusterConfig config;
    private final WalBackedPersistentLog leaderLog;
    private final FollowerClient client;
    private final long waitMillis;
    private final ExecutorService workers;
    private final Map<String, Peer> peers = new ConcurrentHashMap<>();
    private volatile boolean closed;

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
        long index=entry.index();
        Peer peer=peers.get(followerId);
        if(peer==null) throw new IllegalArgumentException("unknown follower");
        if(closed) return new Submission(new FollowerClient.Outcome(FollowerClient.Kind.REMOTE_ERROR,index,0,0,"manager closed"),null);
        if(!peer.active.compareAndSet(false,true))
            return new Submission(new FollowerClient.Outcome(FollowerClient.Kind.BUSY,index,0,0,"follower attempt active"),null);
        try {
            Future<FollowerClient.Outcome> future=workers.submit(()->{
                try {
                    FollowerClient.Outcome result=client.replicate(followerId,entry);
                    if(result==null || result.requestedIndex()!=index || result.kind()==null)
                        return new FollowerClient.Outcome(FollowerClient.Kind.INVALID_RESPONSE,index,0,0,"bad client outcome");
                    if(result.kind()==FollowerClient.Kind.ACK) {
                        if(result.lastReportedPersistedIndex()<index)
                            return new FollowerClient.Outcome(FollowerClient.Kind.INVALID_RESPONSE,index,0,0,"ACK does not cover entry");
                        peer.acceptedRequestIndex.accumulateAndGet(index,Math::max);
                        peer.reportedPersistedIndex.accumulateAndGet(result.lastReportedPersistedIndex(),Math::max);
                    }
                    return result;
                } catch(RuntimeException e) {
                    return new FollowerClient.Outcome(FollowerClient.Kind.REMOTE_ERROR,index,0,0,
                            "worker exception: "+e.getClass().getSimpleName());
                } finally {
                    peer.active.set(false); // actual worker exit, not future cancellation or timeout
                }
            });
            return new Submission(null,future);
        } catch (RejectedExecutionException rejected) {
            peer.active.set(false);
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

    public long highestAcceptedRequestIndex(String id) { return requirePeer(id).acceptedRequestIndex.get(); }
    public long lastReportedPersistedIndex(String id) { return requirePeer(id).reportedPersistedIndex.get(); }
    public boolean isActive(String id) { return requirePeer(id).active.get(); }
    private Peer requirePeer(String id) {
        Peer p=peers.get(id); if(p==null) throw new IllegalArgumentException("unknown follower");return p;
    }

    @Override public synchronized void close() throws IOException {
        closed=true;
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
