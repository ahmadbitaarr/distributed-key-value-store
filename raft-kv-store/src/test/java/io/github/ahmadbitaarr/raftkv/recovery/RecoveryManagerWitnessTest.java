package io.github.ahmadbitaarr.raftkv.recovery;

import io.github.ahmadbitaarr.raftkv.commit.AckTracker;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.leader.CanonicalCommandFingerprint;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.replication.*;
import io.github.ahmadbitaarr.raftkv.server.*;
import io.github.ahmadbitaarr.raftkv.statemachine.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class RecoveryManagerWitnessTest {
    @TempDir Path root;
    private static LogEntry entry(int n,int value){
        var command=Command.put("key"+n,new byte[]{(byte)value});
        return new LogEntry(n,"req-"+n,command,CanonicalCommandFingerprint.of(command));
    }
    private final class Rig implements AutoCloseable {
        final FileWalStore leaderWal,followerWal;
        final WalBackedPersistentLog leaderLog,followerLog;
        final StateMachineApplier applier=new StateMachineApplier(new KvStateMachine());
        final StateMachineApplier followerApplier=new StateMachineApplier(new KvStateMachine());
        final AtomicReference<NodeStatusSnapshot> status;
        final ReplicationOperationGate admission=new ReplicationOperationGate();
        final FollowerReplicationHandler handler;
        final LocalClient client=new LocalClient();
        final ReplicationManager manager;
        final AckTracker tracker;
        final AtomicReference<RecoveryManager.Certificate> accepted=new AtomicReference<>();
        final AtomicReference<IOException> fatal=new AtomicReference<>();
        final RecoveryManager recovery;
        Rig(int leaderCount,int followerCount,int followerCommitted,boolean conflict,boolean extra) throws Exception {
            var lc=ClusterConfigTest.config(root.resolve("leader"),"n1");
            var fc=ClusterConfigTest.config(root.resolve("follower"),"n2");
            leaderWal=FileWalStore.open(lc.walFile());leaderLog=new WalBackedPersistentLog(leaderWal);
            for(int n=1;n<=leaderCount;n++)leaderLog.append(entry(n,n));
            leaderWal.appendCommitDurably(leaderCount);
            RecoveryManager.recoverLocal(leaderLog,leaderWal,applier);
            followerWal=FileWalStore.open(fc.walFile());followerLog=new WalBackedPersistentLog(followerWal);
            for(int n=1;n<=followerCount;n++) {
                var old=n<=leaderCount?entry(n,n):entry(n,88);
                if(conflict && n==1)old=entry(1,99);
                followerLog.append(old);
            }
            if(extra) followerLog.append(entry(followerCount+1,42));
            followerWal.appendCommitDurably(followerCommitted);
            followerApplier.applyThrough(followerCommitted,followerCommitted,followerLog);
            status=new AtomicReference<>(new NodeStatusSnapshot("n2",NodeRole.FOLLOWER,
                    NodeLifecycleState.CONTROL_ONLY,followerLog.lastLogIndex(),followerCommitted,
                    followerCommitted,false));
            handler=new FollowerReplicationHandler(fc,followerLog,followerWal,admission,status::get,
                    n->publish(n,status.get().durableCommitIndex(),status.get().appliedIndex()),
                    followerApplier,
                    n->publish(status.get().lastLogIndex(),n,status.get().appliedIndex()),
                    n->publish(status.get().lastLogIndex(),status.get().durableCommitIndex(),n),
                    fatal::set);
            admission.open();
            tracker=new AckTracker(lc,leaderLog,leaderWal);
            manager=new ReplicationManager(lc,leaderLog,client);
            recovery=new RecoveryManager(lc,leaderLog,leaderWal,applier,manager,client,tracker,
                    accepted::set,fatal::set);
        }
        private void publish(long last,long commit,long applied){
            status.set(new NodeStatusSnapshot("n2",NodeRole.FOLLOWER,NodeLifecycleState.CONTROL_ONLY,
                    last,commit,applied,false));
        }
        final class LocalClient implements FollowerMaintenanceClient {
            final AtomicBoolean unavailable=new AtomicBoolean();
            final AtomicBoolean wrongIdentity=new AtomicBoolean();
            final AtomicInteger truncations=new AtomicInteger();
            public Optional<FollowerStatusProbe.Status> probeStatus(String id){
                if(unavailable.get() || !"n2".equals(id))return Optional.empty();
                var s=status.get();return Optional.of(new FollowerStatusProbe.Status(
                        wrongIdentity.get()?"n3":"n2",NodeRole.FOLLOWER,s.lastLogIndex(),
                        s.durableCommitIndex(),s.appliedIndex(),false));
            }
            public Outcome replicate(String id,LogEntry e){
                try {
                    var d=handler.replicate("n1",e);
                    return switch(d.kind()) {
                        case ACCEPTED -> FollowerClient.ack(e.index(),d.lastPersistedIndex());
                        case INDEX_CONFLICT -> new Outcome(FollowerClient.Kind.INDEX_CONFLICT,e.index(),
                                d.lastPersistedIndex(),d.expectedNextIndex(),"conflict");
                        case INDEX_GAP -> new Outcome(FollowerClient.Kind.INDEX_GAP,e.index(),
                                d.lastPersistedIndex(),d.expectedNextIndex(),"gap");
                        case NOT_READY -> new Outcome(FollowerClient.Kind.NOT_READY,e.index(),0,0,"not ready");
                    };
                }catch(IOException ex){return new Outcome(FollowerClient.Kind.REMOTE_ERROR,e.index(),0,0,ex.toString());}
            }
            public Result advanceCommit(String id,long index){
                try {var d=handler.advanceCommit("n1",index);
                    return new Result(Kind.ACCEPTED,d.commitIndex(),d.commitIndex(),d.appliedIndex(),"committed");}
                catch(Exception ex){return Result.failure(Kind.REJECTED,ex.toString());}
            }
            public Result truncateUncommittedSuffix(String id,long index){
                truncations.incrementAndGet();
                try{var d=handler.truncateUncommittedSuffix("n1",index);
                    return new Result(Kind.ACCEPTED,d.lastLogIndex(),d.commitIndex(),0,"truncated");}
                catch(Exception ex){return Result.failure(Kind.REJECTED,ex.toString());}
            }
        }
        void attempt(int count){for(int n=0;n<count;n++)recovery.runOnce("n2");}
        @Override public void close()throws Exception {
            recovery.close();manager.close();followerLog.close();leaderLog.close();
        }
    }
    @Test void emptyFollowerSequentiallyReplaysAndProvesFullCommittedPrefix()throws Exception {
        try(var rig=new Rig(3,0,0,false,false)){
            rig.attempt(2);
            assertNotNull(rig.accepted.get());
            assertEquals(3,rig.accepted.get().committedIndex());
            assertTrue(rig.recovery.accepts(rig.accepted.get()));
            assertTrue(rig.accepted.get().validFor(rig.leaderLog,rig.leaderWal,rig.applier));
            assertEquals(3,rig.tracker.verifiedMatchingThrough("n2"));
            assertEquals(3,rig.followerWal.durableCommitIndex());
            assertEquals(3,rig.followerApplier.appliedIndex());
            assertArrayEquals(new byte[]{3},rig.followerApplier.diagnosticGetForTest("key3").orElseThrow());
        }
    }
    @Test void existingCommittedFollowerRequiresExactSequentialDuplicateProof()throws Exception {
        try(var rig=new Rig(3,3,3,false,false)){
            assertEquals(-1,rig.tracker.verifiedMatchingThrough("n2"));
            assertNull(rig.accepted.get());
            rig.attempt(1);
            assertNotNull(rig.accepted.get());
            assertEquals(3,rig.tracker.verifiedMatchingThrough("n2"));
        }
    }
    @Test void followerExtraUncommittedTailMustBeDurablyRemovedAndReverified()throws Exception {
        try(var rig=new Rig(2,2,2,false,true)){
            rig.attempt(1);
            assertNull(rig.accepted.get());
            assertEquals(1,rig.client.truncations.get());
            assertEquals(2,rig.followerWal.lastDurableLogIndex());
            assertEquals(-1,rig.tracker.verifiedMatchingThrough("n2"));
            rig.attempt(1);
            assertNotNull(rig.accepted.get());
            assertEquals(2,rig.followerWal.durableCommitIndex());
        }
    }
    @Test void committedPrefixConflictFailsRecoveryRatherThanRewritingIt()throws Exception {
        try(var rig=new Rig(2,2,1,true,false)){
            rig.attempt(1);
            assertEquals(RecoveryManager.State.FAILED,rig.recovery.state());
            assertNotNull(rig.fatal.get());
            assertEquals(2,rig.followerWal.lastDurableLogIndex());
            assertEquals(1,rig.followerWal.durableCommitIndex());
            assertNull(rig.accepted.get());
        }
    }
    @Test void followerCommittedAheadOfAuthoritativeLeaderNeverCertifies()throws Exception {
        try(var rig=new Rig(1,2,2,false,false)){
            rig.attempt(1);
            assertEquals(RecoveryManager.State.FAILED,rig.recovery.state());
            assertNotNull(rig.fatal.get());assertNull(rig.accepted.get());
            assertEquals(2,rig.followerWal.durableCommitIndex());
        }
    }
    @Test void followerUnavailableLeavesRecoveryPendingAndDoesNotInventProof()throws Exception {
        try(var rig=new Rig(2,0,0,false,false)){
            rig.client.unavailable.set(true);rig.attempt(2);
            assertEquals(RecoveryManager.State.AWAITING_FOLLOWER,rig.recovery.state());
            assertEquals(-1,rig.tracker.verifiedMatchingThrough("n2"));
            assertNull(rig.accepted.get());
            rig.client.unavailable.set(false);rig.attempt(2);
            assertNotNull(rig.accepted.get());
        }
    }
    @Test void wrongFollowerIdentityCannotCertifyEvenMatchingIndexes()throws Exception {
        try(var rig=new Rig(1,1,1,false,false)) {
            rig.client.wrongIdentity.set(true);rig.attempt(1);
            assertNull(rig.accepted.get());
            assertEquals(-1,rig.tracker.verifiedMatchingThrough("n2"));
        }
    }
    @Test void validCommittedPrefixWithConflictingUncommittedEntryGetsSafeRewrite()throws Exception {
        try(var rig=new Rig(3,2,1,false,false)) {
            rig.followerLog.truncateAfter(1);
            rig.followerLog.append(entry(2,99));
            rig.status.set(new NodeStatusSnapshot("n2",NodeRole.FOLLOWER,
                    NodeLifecycleState.CONTROL_ONLY,2,1,1,false));
            rig.attempt(1);
            assertNull(rig.accepted.get());
            assertEquals(1,rig.followerWal.lastDurableLogIndex());
            assertEquals(1,rig.followerWal.durableCommitIndex());
            rig.attempt(3);
            assertNotNull(rig.accepted.get());
            assertEquals(3,rig.followerWal.lastDurableLogIndex());
            assertEquals(3,rig.followerWal.durableCommitIndex());
        }
    }
    @Test void issuedCertificateIsInvalidAfterShutdownAndCannotAuthorizeDifferentWal()throws Exception {
        try(var rig=new Rig(1,0,0,false,false)){
            rig.attempt(1);
            var cert=rig.accepted.get();assertNotNull(cert);
            assertFalse(cert.validFor(rig.followerLog,rig.followerWal,rig.followerApplier));
            rig.recovery.closeAdmission();
            assertFalse(rig.recovery.accepts(cert));
        }
    }
    @Test void zeroCommitRecoveredLeaderNeedsAnActuallyEmptyCanonicalFollowerWitness()throws Exception {
        try(var rig=new Rig(0,0,0,false,true)){
            rig.attempt(1);
            assertNull(rig.accepted.get());
            assertEquals(1,rig.client.truncations.get());
            rig.attempt(1);
            assertNotNull(rig.accepted.get());
            assertEquals(0,rig.accepted.get().committedIndex());
            assertEquals(0,rig.followerWal.lastDurableLogIndex());
        }
    }
}
