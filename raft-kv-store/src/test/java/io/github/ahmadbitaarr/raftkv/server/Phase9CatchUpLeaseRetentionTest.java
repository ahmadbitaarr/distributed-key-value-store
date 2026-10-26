package io.github.ahmadbitaarr.raftkv.server;

import io.github.ahmadbitaarr.raftkv.config.*;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.replication.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** F07: NodeServer itself retains its WAL ownership through a blocked catch-up worker. */
class Phase9CatchUpLeaseRetentionTest {
    @TempDir Path walDirectory;

    private static final class UncancellableMaintenance implements FollowerMaintenanceClient {
        final CountDownLatch entered=new CountDownLatch(1);
        final CountDownLatch release=new CountDownLatch(1);
        final CountDownLatch exited=new CountDownLatch(1);
        @Override public Optional<FollowerStatusProbe.Status> probeStatus(String follower) {
            if(!follower.equals("n2"))return Optional.empty();
            entered.countDown();
            try {
                while(release.getCount()>0) {
                    try {release.await();}
                    catch(InterruptedException ignored) { /* blocking RPC not stopped by cancellation */ }
                }
                return Optional.empty();
            } finally {exited.countDown();}
        }
        @Override public Outcome replicate(String id,LogEntry entry) {
            return new Outcome(FollowerClient.Kind.UNAVAILABLE,entry.index(),0,0,"offline");
        }
        @Override public Result advanceCommit(String id,long index) {
            return Result.failure(Kind.UNAVAILABLE,"offline");
        }
        @Override public Result truncateUncommittedSuffix(String id,long index) {
            return Result.failure(Kind.UNAVAILABLE,"offline");
        }
    }

    @Test void nodeShutdownTimeoutRetainsExclusiveWalLeaseUntilActualCatchUpWorkerExit() throws Exception {
        var base=ClusterConfigTest.config(walDirectory,"n1");
        var config=new ClusterConfig(base.clusterId(),base.localNodeId(),base.configuredLeaderId(),
                base.members(),base.localBindHost(),base.localBindPort(),walDirectory,
                200,500,500,50,base.maxInboundMessageBytes());
        var transport=new UncancellableMaintenance();
        var node=new NodeServer(config,0,ignored->transport);
        try {
            node.start();
            assertEquals(NodeLifecycleState.READY,node.statusSnapshot().state());
            assertTrue(transport.entered.await(3,TimeUnit.SECONDS),"background status probe must be in flight");
            IOException incomplete=assertThrows(IOException.class,node::close);
            assertTrue(incomplete.getMessage().contains("catch-up worker"),incomplete.toString());
            assertEquals(NodeLifecycleState.STOPPING,node.statusSnapshot().state());
            assertThrows(IOException.class,()->WalDirectoryLease.acquire(walDirectory),
                    "shutdown timeout may not release WAL ownership while catch-up uses it");
            transport.release.countDown();
            assertTrue(transport.exited.await(3,TimeUnit.SECONDS));
            node.close();
            assertEquals(NodeLifecycleState.STOPPED,node.statusSnapshot().state());
            try(var reacquired=WalDirectoryLease.acquire(walDirectory)) {
                assertTrue(reacquired.isHeld());
            }
        } finally {
            transport.release.countDown();
            if(node.statusSnapshot().state()!=NodeLifecycleState.STOPPED)node.close();
        }
    }
}
