package io.github.ahmadbitaarr.raftkv.replication;

import io.github.ahmadbitaarr.raftkv.commit.AckTracker;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.server.NodeStatusSnapshot;
import io.github.ahmadbitaarr.raftkv.server.NodeLifecycleState;
import io.github.ahmadbitaarr.raftkv.server.NodeRole;
import io.github.ahmadbitaarr.raftkv.wal.FileWalStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class FollowerCatchUpLifecycleTest {
    @TempDir Path dir;
    private static final class BlockingTransport implements FollowerMaintenanceClient {
        final CountDownLatch entered=new CountDownLatch(1);
        final CountDownLatch release=new CountDownLatch(1);
        final AtomicInteger probes=new AtomicInteger();
        public Optional<FollowerStatusProbe.Status> probeStatus(String follower) {
            probes.incrementAndGet();entered.countDown();
            while(release.getCount()>0){
                try{release.await(100,TimeUnit.MILLISECONDS);}
                catch(InterruptedException interrupted){/* deliberately uncancellable I/O seam */}
            }
            return Optional.empty();
        }
        public Outcome replicate(String follower,LogEntry entry) {return new Outcome(FollowerClient.Kind.UNAVAILABLE,entry.index(),0,0,"offline");}
        public Result advanceCommit(String follower,long commit){return Result.failure(Kind.UNAVAILABLE,"offline");}
        public Result truncateUncommittedSuffix(String follower,long target){return Result.failure(Kind.UNAVAILABLE,"offline");}
    }
    private static void enter(BlockingTransport client)throws Exception{assertTrue(client.entered.await(3,TimeUnit.SECONDS));}
    @Test void simultaneousWakeupsAndManualSessionsCannotOverlapSameFollower()throws Exception{
        var cfg=ClusterConfigTest.config(dir,"n1");
        try(var wal=FileWalStore.open(cfg.walFile());var log=new WalBackedPersistentLog(wal)) {
            var client=new BlockingTransport();
            try(var manager=new ReplicationManager(cfg,log,client);
                var catchup=new FollowerCatchUpCoordinator(cfg,log,()->new NodeStatusSnapshot("n1",NodeRole.LEADER,
                    NodeLifecycleState.CONTROL_ONLY,log.lastLogIndex(),0,0,false),manager,client,
                        new AckTracker(cfg,log,wal),()->false)) {
                var pool=Executors.newFixedThreadPool(2);
                try {
                    Future<?> first=pool.submit(()->catchup.runOnce("n2"));
                    enter(client);
                    Future<?> second=pool.submit(()->catchup.runOnce("n2"));
                    second.get(2,TimeUnit.SECONDS);
                    assertEquals(1,client.probes.get());
                    assertTrue(manager.isActive("n2"));
                    client.release.countDown();first.get(2,TimeUnit.SECONDS);
                    assertFalse(manager.isActive("n2"));
                }finally{client.release.countDown();pool.shutdownNow();}
            }
        }
    }
    @Test void shutdownTimeoutRetainsActiveCatchUpTransportOwnershipUntilActualExit()throws Exception{
        var cfg=ClusterConfigTest.config(dir,"n1");
        try(var wal=FileWalStore.open(cfg.walFile());var log=new WalBackedPersistentLog(wal)) {
            var client=new BlockingTransport();
            var manager=new ReplicationManager(cfg,log,client);
            var catchup=new FollowerCatchUpCoordinator(cfg,log,()->new NodeStatusSnapshot("n1",NodeRole.LEADER,
                    NodeLifecycleState.CONTROL_ONLY,log.lastLogIndex(),0,0,false),manager,client,
                    new AckTracker(cfg,log,wal),()->false);
            try {
                catchup.start();enter(client);
                assertThrows(IOException.class,catchup::close,
                        "uncancellable operation cannot be treated as terminated by shutdownNow");
                assertTrue(manager.isActive("n2"));
                assertThrows(IOException.class,manager::close,
                        "active worker ownership cannot be silently released");
                assertEquals(0,wal.durableCommitIndex());
            } finally {
                client.release.countDown();
                // The owner is released only after the actual operation exits.
                long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
                while(manager.isActive("n2") && System.nanoTime()<deadline)Thread.sleep(10);
                catchup.close();manager.close();
            }
            assertFalse(manager.isActive("n2"));
        }
    }
}
