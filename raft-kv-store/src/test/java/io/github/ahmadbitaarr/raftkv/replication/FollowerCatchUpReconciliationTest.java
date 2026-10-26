package io.github.ahmadbitaarr.raftkv.replication;

import io.github.ahmadbitaarr.raftkv.commit.AckTracker;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.server.*;
import io.github.ahmadbitaarr.raftkv.statemachine.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import static org.junit.jupiter.api.Assertions.*;

class FollowerCatchUpReconciliationTest {
    @TempDir Path root;
    private static LogEntry e(long i,int value) {
        return new LogEntry(i,"entry-"+i,Command.put("key"+i,new byte[]{(byte)value}),new byte[]{(byte)value});
    }
    private final class Rig implements AutoCloseable {
        final FileWalStore leaderWal;
        final WalBackedPersistentLog leaderLog;
        final FileWalStore followerWal;
        final WalBackedPersistentLog followerLog;
        final StateMachineApplier followerApplier=new StateMachineApplier(new KvStateMachine());
        final ReplicationOperationGate gate=new ReplicationOperationGate();
        final AtomicReference<NodeStatusSnapshot> status;
        final FollowerReplicationHandler handler;
        final LocalClient client;
        final AckTracker tracker;
        final ReplicationManager manager;
        final FollowerCatchUpCoordinator coordinator;
        final AtomicReference<NodeStatusSnapshot> leaderPublished;
        final AtomicReference<Supplier<NodeStatusSnapshot>> leaderSnapshotReader=new AtomicReference<>();
        final AtomicBoolean unresolved=new AtomicBoolean();
        Rig(int leaderCommits,int followerCommits,boolean conflictingSuffix)throws Exception {
            var lc=ClusterConfigTest.config(root.resolve("n1"),"n1");
            var fc=ClusterConfigTest.config(root.resolve("n2"),"n2");
            leaderWal=FileWalStore.open(lc.walFile());leaderLog=new WalBackedPersistentLog(leaderWal);
            for(int i=1;i<=3;i++)leaderLog.append(e(i,i));
            leaderWal.appendCommitDurably(leaderCommits);
            followerWal=FileWalStore.open(fc.walFile());followerLog=new WalBackedPersistentLog(followerWal);
            for(int i=1;i<=3;i++){
                if(i<=followerCommits || (conflictingSuffix && i==followerCommits+1))
                    followerLog.append(e(i,conflictingSuffix && i==followerCommits+1 ? 99 : i));
            }
            followerWal.appendCommitDurably(followerCommits);
            followerApplier.applyThrough(followerCommits,followerCommits,followerLog);
            status=new AtomicReference<>(new NodeStatusSnapshot("n2",NodeRole.FOLLOWER,
                    NodeLifecycleState.CONTROL_ONLY,followerLog.lastLogIndex(),followerCommits,followerCommits,false));
            handler=new FollowerReplicationHandler(fc,followerLog,followerWal,gate,status::get,
                    n->publish(n,status.get().durableCommitIndex(),status.get().appliedIndex()),followerApplier,
                    n->publish(status.get().lastLogIndex(),n,status.get().appliedIndex()),
                    n->publish(status.get().lastLogIndex(),status.get().durableCommitIndex(),n),
                    cause->{throw new AssertionError("unexpected fatal",cause);});
            gate.open();client=new LocalClient();
            manager=new ReplicationManager(lc,leaderLog,client);
            tracker=new AckTracker(lc,leaderLog,leaderWal);
            leaderPublished=new AtomicReference<>(new NodeStatusSnapshot("n1",NodeRole.LEADER,
                    NodeLifecycleState.READY,leaderLog.lastLogIndex(),leaderCommits,0,true));
            leaderSnapshotReader.set(leaderPublished::get);
            coordinator=new FollowerCatchUpCoordinator(lc,leaderLog,
                    ()->leaderSnapshotReader.get().get(),manager,client,tracker,unresolved::get);
        }
        void publish(long last,long committed,long applied){
            status.set(new NodeStatusSnapshot("n2",NodeRole.FOLLOWER,NodeLifecycleState.CONTROL_ONLY,
                    last,committed,applied,false));
        }
        private final class LocalClient implements FollowerMaintenanceClient {
            final AtomicInteger rejectNextEntryIndex=new AtomicInteger(-1);
            final AtomicBoolean invalidStatus=new AtomicBoolean();
            final AtomicBoolean forgedAckAtOne=new AtomicBoolean();
            final AtomicInteger truncationCalls=new AtomicInteger();
            volatile java.util.concurrent.CountDownLatch statusCaptured;
            volatile java.util.concurrent.CountDownLatch releaseStatus;
            final AtomicReference<Runnable> firstReplicationHook=new AtomicReference<>();
            public Optional<FollowerStatusProbe.Status> probeStatus(String id){
                if (invalidStatus.get()) return Optional.of(new FollowerStatusProbe.Status(
                        "n3",NodeRole.FOLLOWER,1,0,0,false));
                var s=status.get();
                var result=new FollowerStatusProbe.Status("n2",NodeRole.FOLLOWER,
                        s.lastLogIndex(),s.durableCommitIndex(),s.appliedIndex(),false);
                var captured=statusCaptured;
                if(captured!=null) {
                    captured.countDown();
                    try {
                        if(!releaseStatus.await(3,java.util.concurrent.TimeUnit.SECONDS))
                            throw new AssertionError("status publication test barrier timed out");
                    }catch(InterruptedException e) {
                        Thread.currentThread().interrupt();throw new AssertionError(e);
                    }
                }
                return Optional.of(result);
            }
            public Outcome replicate(String id,LogEntry entry) {
                Runnable hook=firstReplicationHook.getAndSet(null);
                if(hook!=null)hook.run();
                if (entry.index()==1 && forgedAckAtOne.get())
                    return FollowerClient.ack(2,2); // wrong requested index cannot extend exact prefix proof
                if(rejectNextEntryIndex.compareAndSet((int)entry.index(),-1))
                    return new Outcome(FollowerClient.Kind.UNAVAILABLE,entry.index(),0,0,"simulated transport break");
                try {
                    var d=handler.replicate("n1",entry);
                    return switch(d.kind()) {
                        case ACCEPTED -> FollowerClient.ack(entry.index(),d.lastPersistedIndex());
                        case INDEX_CONFLICT -> new Outcome(FollowerClient.Kind.INDEX_CONFLICT,entry.index(),d.lastPersistedIndex(),d.expectedNextIndex(),"conflict");
                        case INDEX_GAP -> new Outcome(FollowerClient.Kind.INDEX_GAP,entry.index(),d.lastPersistedIndex(),d.expectedNextIndex(),"gap");
                        case NOT_READY -> new Outcome(FollowerClient.Kind.NOT_READY,entry.index(),d.lastPersistedIndex(),0,"not ready");
                    };
                } catch(Exception problem){return new Outcome(FollowerClient.Kind.REMOTE_ERROR,entry.index(),0,0,problem.getMessage());}
            }
            public Result advanceCommit(String id,long target) {
                try{var d=handler.advanceCommit("n1",target);
                    return new Result(Kind.ACCEPTED,d.commitIndex(),d.commitIndex(),d.appliedIndex(),"applied");}
                catch(Exception ex){return Result.failure(Kind.REJECTED,ex.toString());}
            }
            public Result truncateUncommittedSuffix(String id,long index) {
                truncationCalls.incrementAndGet();
                try{var d=handler.truncateUncommittedSuffix("n1",index);
                    return new Result(Kind.ACCEPTED,d.lastLogIndex(),d.commitIndex(),0,"rewritten");}
                catch(Exception ex){return Result.failure(Kind.REJECTED,ex.toString());}
            }
        }
        @Override public void close()throws Exception{coordinator.close();manager.close();followerLog.close();leaderLog.close();}
    }
    @Test void concurrentLeaderCommitPublicationBeforeStatusProcessingDoesNotMarkHealthyFollowerUnsafe()
            throws Exception {
        try(var rig=new Rig(1,2,false)) {
            // The follower status RPC returns a valid committed index 2, while the
            // leader advances from 1 to 2 before the reply is processed.
            var captured=new CountDownLatch(1);
            var release=new CountDownLatch(1);
            rig.client.statusCaptured=captured;
            rig.client.releaseStatus=release;
            ExecutorService threads=Executors.newSingleThreadExecutor();
            try {
                Future<?> session=threads.submit(()->rig.coordinator.runOnce("n2"));
                assertTrue(captured.await(3,TimeUnit.SECONDS));
                rig.leaderWal.appendCommitDurably(2);
                rig.leaderPublished.updateAndGet(old->new NodeStatusSnapshot("n1",NodeRole.LEADER,
                        old.state(),old.lastLogIndex(),2,old.appliedIndex(),old.ready()));
                release.countDown();
                session.get(4,TimeUnit.SECONDS);
                assertFalse(rig.coordinator.diagnosticSnapshot().get("n2").startsWith("UNSAFE"));
                assertEquals("SYNCHRONIZED",rig.coordinator.diagnosticSnapshot().get("n2"));
                assertEquals(2,rig.tracker.verifiedMatchingThrough("n2"));
                assertEquals(2,rig.followerWal.durableCommitIndex());
                assertEquals(2,rig.followerApplier.appliedIndex());
            } finally {
                release.countDown();threads.shutdownNow();
            }
        }
    }

    @Test void leaderCommitPublicationRacingUnsafeDecisionIsRecheckedBeforePermanentFailure()
            throws Exception {
        try(var rig=new Rig(2,2,false)) {
            // Force of COMMIT(2) has succeeded, but the first published status
            // sample still represents the previous COMMIT(1). The atomic
            // publication advances before the coordinator's safety recheck.
            var previous=new NodeStatusSnapshot("n1",NodeRole.LEADER,NodeLifecycleState.READY,
                    3,1,0,true);
            var current=rig.leaderPublished.get();
            var calls=new AtomicInteger();
            rig.leaderSnapshotReader.set(()->{
                if(calls.getAndIncrement()==0)return previous;
                return current;
            });
            rig.coordinator.runOnce("n2");
            assertEquals(2,calls.get());
            assertFalse(rig.coordinator.diagnosticSnapshot().get("n2").startsWith("UNSAFE"));
            assertEquals(2,rig.tracker.verifiedMatchingThrough("n2"));
            assertEquals(2,rig.followerWal.durableCommitIndex());
        }
    }

    @Test void commitPublishedMidSessionIsAppliedOnlyAfterNextCoherentSnapshot()throws Exception {
        try(var rig=new Rig(2,0,false)) {
            // Publication to 3 happens after the session sampled commit 2 but
            // while it is replicating that snapshot's first entry.
            rig.client.firstReplicationHook.set(()->{
                try {rig.leaderWal.appendCommitDurably(3);}
                catch(java.io.IOException e){throw new AssertionError(e);}
                rig.leaderPublished.updateAndGet(old->new NodeStatusSnapshot("n1",NodeRole.LEADER,
                        old.state(),old.lastLogIndex(),3,old.appliedIndex(),old.ready()));
            });
            rig.coordinator.runOnce("n2");
            assertEquals(3,rig.leaderPublished.get().durableCommitIndex());
            assertEquals(2,rig.followerWal.durableCommitIndex());
            assertEquals(2,rig.followerApplier.appliedIndex());
            assertEquals(2,rig.tracker.verifiedMatchingThrough("n2"));
            // A subsequent bounded session observes the new published index.
            rig.coordinator.runOnce("n2");
            assertEquals(3,rig.followerWal.durableCommitIndex());
            assertEquals(3,rig.followerApplier.appliedIndex());
            assertEquals(3,rig.tracker.verifiedMatchingThrough("n2"));
        }
    }

    @Test void verifiedPrefixAllowsUncommittedConflictRewriteAndExactReverification()throws Exception {
        try(var rig=new Rig(3,2,true)){
            rig.coordinator.runOnce("n2");
            assertEquals(2,rig.followerLog.lastLogIndex());
            assertEquals(2,rig.followerWal.durableCommitIndex());
            assertEquals(-1,rig.tracker.verifiedMatchingThrough("n2"));
            rig.coordinator.runOnce("n2");
            assertEquals(3,rig.followerLog.lastLogIndex());
            assertEquals(e(3,3),rig.followerLog.get(3).orElseThrow());
            assertEquals(3,rig.followerWal.durableCommitIndex());
            assertEquals(3,rig.followerApplier.appliedIndex());
            assertArrayEquals(new byte[]{3},rig.followerApplier.diagnosticGetForTest("key3").orElseThrow());
        }
    }
    @Test void followerCommittedBeyondLeaderNeverGetsReconciledOrTruncated()throws Exception {
        try(var rig=new Rig(1,2,false)){
            rig.coordinator.runOnce("n2");
            assertTrue(rig.coordinator.diagnosticSnapshot().get("n2").startsWith("UNSAFE"));
            assertEquals(2,rig.followerWal.durableCommitIndex());
            assertEquals(2,rig.followerLog.lastLogIndex());
            assertEquals(-1,rig.tracker.verifiedMatchingThrough("n2"));
        }
    }
    @Test void unknownFollowerIdentityNeverAuthorizesSuffixTruncation() throws Exception {
        try (var rig=new Rig(3,0,true)) {
            // Follower contains a conflicting entry at index 1, but its identity is untrusted.
            rig.client.invalidStatus.set(true);
            rig.coordinator.runOnce("n2");
            assertEquals(-1,rig.tracker.verifiedMatchingThrough("n2"));
            assertEquals(0,rig.client.truncationCalls.get());
            assertEquals(e(1,99),rig.followerLog.get(1).orElseThrow());
            assertEquals(0,rig.followerWal.durableCommitIndex());
        }
    }
    @Test void forgedAckCannotAuthorizeTruncationOfUnverifiedPrefix() throws Exception {
        try (var rig=new Rig(3,0,true)) {
            // A valid identity alone does not authorize truncation after a forged ACK.
            rig.client.forgedAckAtOne.set(true);
            rig.coordinator.runOnce("n2");
            assertEquals(0,rig.tracker.verifiedMatchingThrough("n2"));
            assertEquals(0,rig.client.truncationCalls.get());
            assertEquals(e(1,99),rig.followerLog.get(1).orElseThrow());
            assertEquals(0,rig.followerWal.durableCommitIndex());
        }
    }
    @Test void interruptedCatchUpContinuesFromItsPreviouslyVerifiedPrefix()throws Exception {
        try(var rig=new Rig(3,0,false)){
            rig.client.rejectNextEntryIndex.set(2);
            rig.coordinator.runOnce("n2");
            assertEquals(1,rig.followerLog.lastLogIndex());
            assertEquals(0,rig.followerWal.durableCommitIndex());
            assertEquals(1,rig.tracker.verifiedMatchingThrough("n2"));
            rig.coordinator.runOnce("n2");
            assertEquals(3,rig.followerLog.lastLogIndex());
            assertEquals(3,rig.followerWal.durableCommitIndex());
            assertEquals(3,rig.followerApplier.appliedIndex());
        }
    }
    @Test void unresolvedLeaderSuffixCannotBeRetroactivelyCommittedByCatchUp()throws Exception {
        try(var rig=new Rig(2,2,true)){
            rig.unresolved.set(true);
            rig.coordinator.runOnce("n2");
            assertEquals(2,rig.leaderWal.durableCommitIndex());
            assertEquals(2,rig.followerWal.durableCommitIndex());
            assertEquals(3,rig.followerLog.lastLogIndex());
            assertEquals(0,rig.tracker.verifiedMatchingThrough("n2")>2?1:0);
        }
    }
}
