package io.github.ahmadbitaarr.raftkv.server;

import io.grpc.Server;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.leader.LeaderReadCoordinator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class Phase8ReadShutdownTest {
    @TempDir Path dir;
    private static final class InstantServer extends Server {
        private volatile boolean stopped;
        @Override public Server start(){return this;}
        @Override public Server shutdown(){stopped=true;return this;}
        @Override public Server shutdownNow(){stopped=true;return this;}
        @Override public boolean isShutdown(){return stopped;}
        @Override public boolean isTerminated(){return stopped;}
        @Override public boolean awaitTermination(long timeout,TimeUnit unit){return stopped;}
        @Override public void awaitTermination(){}
        @Override public int getPort(){return 50101;}
    }
    @Test void blockedStateAccessRetainsWalOwnershipUntilReadPermitCompletes() throws Exception {
        var n=new NodeServer(ClusterConfigTest.config(dir,"n1"),0,(c,p,client,peer)->new InstantServer());
        n.start();assertTrue(n.statusSnapshot().ready());
        ExecutorService executor=Executors.newFixedThreadPool(3);
        CountDownLatch applierHeld=new CountDownLatch(1),unlockApplier=new CountDownLatch(1);
        try {
            Future<?> locker=executor.submit(()->{
                synchronized(n.readApplierForTest()){
                    applierHeld.countDown();
                    try {unlockApplier.await(10,TimeUnit.SECONDS);}
                    catch(InterruptedException e){Thread.currentThread().interrupt();}
                }
            });
            assertTrue(applierHeld.await(5,TimeUnit.SECONDS));
            var reading=executor.submit(()->n.readCoordinatorForTest().read("missing"));
            long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
            while(n.readCoordinatorForTest().inFlightReads()!=1 && System.nanoTime()<until)Thread.onSpinWait();
            assertEquals(1,n.readCoordinatorForTest().inFlightReads());
            Future<IOException> shutdown=executor.submit(()->{
                try {n.close();return null;}catch(IOException failure){return failure;}
            });
            var exception=shutdown.get(8,TimeUnit.SECONDS);
            assertNotNull(exception,"bounded shutdown must report un-drained read");
            assertTrue(exception.getMessage().contains("read"));
            assertEquals(NodeLifecycleState.STOPPING,n.statusSnapshot().state());
            assertThrows(IOException.class,()->WalDirectoryLease.acquire(dir),"active read retains WAL lease");
            assertEquals(LeaderReadCoordinator.Code.NOT_READY,n.readCoordinatorForTest().read("new").code());
            unlockApplier.countDown();locker.get(5,TimeUnit.SECONDS);
            assertEquals(LeaderReadCoordinator.Code.NOT_READY,reading.get(5,TimeUnit.SECONDS).code());
            n.close();assertEquals(NodeLifecycleState.STOPPED,n.statusSnapshot().state());
            try(var lease=WalDirectoryLease.acquire(dir)){assertTrue(lease.isHeld());}
        }finally{unlockApplier.countDown();executor.shutdownNow();assertTrue(executor.awaitTermination(5,TimeUnit.SECONDS));}
    }
    @Test void freshLeaderReadGateClosesOnCleanShutdown() throws Exception {
        try(var n=new NodeServer(ClusterConfigTest.config(dir,"n1"),0,(c,p,client,peer)->new InstantServer())) {
            n.start();
            assertEquals(LeaderReadCoordinator.Code.NOT_FOUND,n.readCoordinatorForTest().read("x").code());
            n.close();
            assertEquals(LeaderReadCoordinator.Code.NOT_READY,n.readCoordinatorForTest().read("x").code());
            assertFalse(n.statusSnapshot().ready());
        }
    }
}
