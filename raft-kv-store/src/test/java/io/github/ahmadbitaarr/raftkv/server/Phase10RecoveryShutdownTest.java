package io.github.ahmadbitaarr.raftkv.server;

import io.github.ahmadbitaarr.raftkv.config.*;
import io.github.ahmadbitaarr.raftkv.leader.CanonicalCommandFingerprint;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.replication.*;
import io.github.ahmadbitaarr.raftkv.wal.FileWalStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class Phase10RecoveryShutdownTest {
    @TempDir Path root;
    private ClusterConfig cfg(){
        var m=ClusterConfigTest.members();
        return new ClusterConfig("phase10", "n1", "n1",m,"127.0.0.1",m.get("n1").port(),
                root.resolve("leader"),1000,500,300,200,12582912);
    }
    private void populate()throws Exception {
        var command=Command.put("x",new byte[]{1});
        try(var wal=FileWalStore.open(cfg().walFile())) {
            wal.appendEntryDurably(new LogEntry(1,"original",command,CanonicalCommandFingerprint.of(command)));
            wal.appendCommitDurably(1);
        }
    }
    private static class BlockedProbe implements FollowerMaintenanceClient {
        final CountDownLatch entered=new CountDownLatch(1);
        final CountDownLatch exit=new CountDownLatch(1);
        public Optional<FollowerStatusProbe.Status> probeStatus(String id){
            entered.countDown();
            boolean done=false;
            while(!done)try{exit.await();done=true;}catch(InterruptedException ignored){ /* blocked transport ignores cancellation */ }
            return Optional.empty();
        }
        public Outcome replicate(String id,LogEntry e){
            return new Outcome(FollowerClient.Kind.UNAVAILABLE,e.index(),0,0,"offline");
        }
        public Result advanceCommit(String id,long i){return Result.failure(Kind.UNAVAILABLE,"offline");}
        public Result truncateUncommittedSuffix(String id,long i){return Result.failure(Kind.UNAVAILABLE,"offline");}
    }
    @Test void shutdownTimeoutRetainsWalLeaseUntilActualRecoveryWorkerExits()throws Exception {
        populate();var fake=new BlockedProbe();
        var node=new NodeServer(cfg(),0,c->fake);
        try {
            node.start();
            assertTrue(fake.entered.await(3,TimeUnit.SECONDS));
            assertEquals(NodeLifecycleState.CONTROL_ONLY,node.statusSnapshot().state());
            IOException pending=assertThrows(IOException.class,node::close);
            assertTrue(pending.getMessage().contains("recovery workers still active"));
            assertThrows(IOException.class,()->WalDirectoryLease.acquire(cfg().walDirectory()));
            fake.exit.countDown();
            node.close();
            assertEquals(NodeLifecycleState.STOPPED,node.statusSnapshot().state());
            try(var newlyOwned=WalDirectoryLease.acquire(cfg().walDirectory())){
                assertTrue(newlyOwned.isHeld());
            }
        } finally {fake.exit.countDown();node.close();}
    }
    @Test void followerCommittedAheadCausesFatalNotReadyInsteadOfFalseRecoveryCertificate()throws Exception {
        populate();
        FollowerMaintenanceClient fake=new FollowerMaintenanceClient(){
            public Optional<FollowerStatusProbe.Status> probeStatus(String id) {
                return Optional.of(new FollowerStatusProbe.Status(id,NodeRole.FOLLOWER,2,2,2,false));
            }
            public Outcome replicate(String id,LogEntry e){return FollowerClient.ack(e.index(),e.index());}
            public Result advanceCommit(String id,long i){return Result.failure(Kind.REJECTED,"not allowed");}
            public Result truncateUncommittedSuffix(String id,long i){return Result.failure(Kind.REJECTED,"not allowed");}
        };
        try(var node=new NodeServer(cfg(),0,c->fake)) {
            node.start();
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(4);
            while(node.statusSnapshot().state()!=NodeLifecycleState.FAILED && System.nanoTime()<deadline)
                Thread.sleep(10);
            assertEquals(NodeLifecycleState.FAILED,node.statusSnapshot().state());
            assertFalse(node.statusSnapshot().ready());
            assertEquals(1,node.statusSnapshot().appliedIndex());
        } catch(IOException expectedFatalOnClose) {
            assertTrue(expectedFatalOnClose.getMessage().contains("fatal"));
        }
    }
}
