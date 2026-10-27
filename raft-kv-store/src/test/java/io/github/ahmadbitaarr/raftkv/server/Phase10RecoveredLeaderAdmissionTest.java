package io.github.ahmadbitaarr.raftkv.server;

import io.github.ahmadbitaarr.raftkv.config.*;
import io.github.ahmadbitaarr.raftkv.leader.*;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.replication.*;
import io.github.ahmadbitaarr.raftkv.statemachine.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

/** Real file-backed follower transport fake and NodeServer lifecycle; no proto/Netty assumptions. */
class Phase10RecoveredLeaderAdmissionTest {
    @TempDir Path root;
    private ClusterConfig cfg(String id){return ClusterConfigTest.config(root.resolve(id),id);}
    private static LogEntry entry(long i,String id,Command c){
        return new LogEntry(i,id,c,CanonicalCommandFingerprint.of(c));
    }
    private void prepare()throws Exception {
        var c=Command.put("x",new byte[]{1,0});
        for(String id:List.of("n1","n2")) {
            try(var wal=FileWalStore.open(cfg(id).walFile())){
                wal.appendEntryDurably(entry(1,"prior",c));wal.appendCommitDurably(1);
            }
        }
    }
    private static class DiskPeer implements FollowerMaintenanceClient, AutoCloseable {
        final FileWalStore wal;
        final WalBackedPersistentLog log;
        final StateMachineApplier applier=new StateMachineApplier(new KvStateMachine());
        final AtomicBoolean online=new AtomicBoolean(false);
        DiskPeer(Path walPath)throws Exception {
            wal=FileWalStore.open(walPath);log=new WalBackedPersistentLog(wal);
            applier.applyThrough(wal.durableCommitIndex(),wal.durableCommitIndex(),log);
        }
        public Optional<FollowerStatusProbe.Status> probeStatus(String id){
            if(!online.get() || !id.equals("n2"))return Optional.empty();
            return Optional.of(new FollowerStatusProbe.Status("n2",NodeRole.FOLLOWER,
                    log.lastLogIndex(),wal.durableCommitIndex(),applier.appliedIndex(),false));
        }
        public FollowerClient.Outcome replicate(String id,LogEntry e){
            if(!online.get())return new FollowerClient.Outcome(FollowerClient.Kind.UNAVAILABLE,e.index(),0,0,"offline");
            try {
                if(e.index()<=log.lastLogIndex()) {
                    return log.get(e.index()).filter(e::equals).isPresent()
                            ?FollowerClient.ack(e.index(),log.lastLogIndex())
                            :new FollowerClient.Outcome(FollowerClient.Kind.INDEX_CONFLICT,e.index(),
                                    log.lastLogIndex(),0,"conflict");
                }
                log.append(e);
                return FollowerClient.ack(e.index(),log.lastLogIndex());
            }catch(Exception ex){return new FollowerClient.Outcome(FollowerClient.Kind.REMOTE_ERROR,e.index(),0,0,ex.toString());}
        }
        public Result advanceCommit(String id,long target){
            try {
                wal.appendCommitDurably(target);
                long applied=applier.applyThrough(target,wal.durableCommitIndex(),log);
                return new Result(Kind.ACCEPTED,log.lastLogIndex(),wal.durableCommitIndex(),applied,"forced");
            }catch(Exception ex){return Result.failure(Kind.FAILED,ex.toString());}
        }
        public Result truncateUncommittedSuffix(String id,long target){
            try{log.truncateAfter(target);
                return new Result(Kind.ACCEPTED,log.lastLogIndex(),wal.durableCommitIndex(),
                        applier.appliedIndex(),"canonical");}
            catch(Exception ex){return Result.failure(Kind.FAILED,ex.toString());}
        }
        @Override public void close()throws java.io.IOException{log.close();}
    }
    @Test void recoveredLeaderRequiresDurableWitnessBeforeGetAndFreshMutationAdmission()throws Exception {
        prepare();
        try(var peer=new DiskPeer(cfg("n2").walFile());
            var node=new NodeServer(cfg("n1"),0,c->peer)) {
            node.start();
            assertEquals(NodeLifecycleState.CONTROL_ONLY,node.statusSnapshot().state());
            assertEquals(1,node.statusSnapshot().appliedIndex());
            assertEquals(LeaderReadCoordinator.Code.NOT_READY,node.readCoordinatorForTest().read("x").code());
            assertEquals(LeaderCoordinator.Code.NOT_READY,node.leaderCoordinatorForTest().mutate(
                    "unverified",Command.put("x",new byte[]{99})).code());
            peer.online.set(true);
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(4);
            while(!node.statusSnapshot().ready() && System.nanoTime()<deadline)Thread.sleep(10);
            assertTrue(node.statusSnapshot().ready());
            assertEquals(LeaderReadCoordinator.Code.OK,node.readCoordinatorForTest().read("x").code());
            assertArrayEquals(new byte[]{1,0},node.readCoordinatorForTest().read("x").value().orElseThrow());
            assertEquals(LeaderCoordinator.Code.NOT_READY,node.leaderCoordinatorForTest().mutate(
                    "prior",Command.put("x",new byte[]{77})).code());
            assertEquals(LeaderCoordinator.Code.OK,node.leaderCoordinatorForTest().mutate(
                    "new",Command.put("x",new byte[]{2})).code());
            assertEquals(2,node.statusSnapshot().lastLogIndex());
            assertEquals(2,node.statusSnapshot().durableCommitIndex());
            assertEquals(2,node.statusSnapshot().appliedIndex());
            assertArrayEquals(new byte[]{2},node.readCoordinatorForTest().read("x").value().orElseThrow());
        }
    }
    @Test void shutdownWhileRecoveryIsUnavailableCannotReopenAdmissions()throws Exception {
        prepare();
        try(var peer=new DiskPeer(cfg("n2").walFile());var node=new NodeServer(cfg("n1"),0,c->peer)) {
            node.start();
            assertEquals(NodeLifecycleState.CONTROL_ONLY,node.statusSnapshot().state());
            node.close();peer.online.set(true);
            assertEquals(NodeLifecycleState.STOPPED,node.statusSnapshot().state());
            assertFalse(node.statusSnapshot().ready());
            assertEquals(LeaderReadCoordinator.Code.NOT_READY,node.readCoordinatorForTest().read("x").code());
            assertEquals(LeaderCoordinator.Code.NOT_READY,node.leaderCoordinatorForTest().mutate(
                    "later",Command.put("x",new byte[]{3})).code());
        }
    }

    @Test void fatalRecoveredAuthorityFailureStopsNewReadsAndWritesWithoutAdvancingWal()throws Exception {
        prepare();
        try(var peer=new DiskPeer(cfg("n2").walFile());var node=new NodeServer(cfg("n1"),0,c->peer)) {
            peer.online.set(true);node.start();
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(4);
            while(!node.statusSnapshot().ready() && System.nanoTime()<deadline)Thread.sleep(10);
            assertTrue(node.statusSnapshot().ready());
            node.leaderCoordinatorForTest().failExternally(new java.io.IOException("committed-history conflict"));
            assertEquals(LeaderReadCoordinator.Code.NOT_READY,node.readCoordinatorForTest().read("x").code());
            assertEquals(LeaderCoordinator.Code.NOT_READY,node.leaderCoordinatorForTest().mutate(
                    "unsafe",Command.put("x",new byte[]{99})).code());
            assertEquals(1,node.statusSnapshot().lastLogIndex());
            assertEquals(1,node.statusSnapshot().durableCommitIndex());
        }
    }
}
