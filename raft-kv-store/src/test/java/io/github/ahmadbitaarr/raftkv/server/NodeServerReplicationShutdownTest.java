package io.github.ahmadbitaarr.raftkv.server;

import io.grpc.Server;
import io.github.ahmadbitaarr.raftkv.config.*;
import io.github.ahmadbitaarr.raftkv.replication.ReplicationOperationGate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class NodeServerReplicationShutdownTest {
    @TempDir Path dir;
    private static final class CompletedGrpcServer extends Server {
        private boolean shutdown;
        @Override public Server start(){return this;}
        @Override public Server shutdown(){shutdown=true;return this;}
        @Override public Server shutdownNow(){shutdown=true;return this;}
        @Override public boolean isShutdown(){return shutdown;}
        @Override public boolean isTerminated(){return shutdown;}
        @Override public void awaitTermination(){}
        @Override public boolean awaitTermination(long timeout,TimeUnit unit){return shutdown;}
        @Override public int getPort(){return 50102;}
    }
    private NodeServer node(long ms){
        var example=ClusterConfigTest.config(dir,"n2");
        var cfg=new ClusterConfig(example.clusterId(),example.localNodeId(),example.configuredLeaderId(),
                example.members(),example.localBindHost(),example.localBindPort(),dir,100,100,100,(int)ms,12582912);
        return new NodeServer(cfg,0,(c,p,client,replication)->new CompletedGrpcServer());
    }
    @Test void shutdownWaitsForActualStorageOperationCompletion()throws Exception {
        NodeServer node=node(1500);node.start();
        var permit=node.operationGateForTest().tryAcquire();assertNotNull(permit);
        ExecutorService worker=Executors.newSingleThreadExecutor();
        try {
            var shutdown=worker.submit(()->{node.close();return true;});
            long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(2);
            while(node.statusSnapshot().state()!=NodeLifecycleState.STOPPING && System.nanoTime()<until)Thread.onSpinWait();
            assertEquals(NodeLifecycleState.STOPPING,node.statusSnapshot().state());
            assertFalse(shutdown.isDone());
            assertThrows(IOException.class,()->WalDirectoryLease.acquire(dir));
            permit.close();
            assertTrue(shutdown.get(2,TimeUnit.SECONDS));
            try(var lease=WalDirectoryLease.acquire(dir)){assertTrue(lease.isHeld());}
        }finally{permit.close();worker.shutdownNow();}
    }
    @Test void shutdownTimeoutPreservesWalLeaseUntilHandlerFinishes()throws Exception {
        NodeServer node=node(30);node.start();
        var permit=node.operationGateForTest().tryAcquire();assertNotNull(permit);
        try {
            assertThrows(IOException.class,node::close);
            assertEquals(NodeLifecycleState.STOPPING,node.statusSnapshot().state());
            assertThrows(IOException.class,()->WalDirectoryLease.acquire(dir));
            permit.close();node.close();
            assertEquals(NodeLifecycleState.STOPPED,node.statusSnapshot().state());
            try(var lease=WalDirectoryLease.acquire(dir)){assertTrue(lease.isHeld());}
        }finally{permit.close();}
    }
    @Test void cancelledGrpcCallDoesNotPrematurelyCloseWal()throws Exception {
        NodeServer node=node(40);node.start();
        var permit=node.operationGateForTest().tryAcquire();assertNotNull(permit);
        CountDownLatch release=new CountDownLatch(1),entered=new CountDownLatch(1);
        var pool=Executors.newSingleThreadExecutor();
        try {
            Future<?> cancelledCall=pool.submit(()->{
                entered.countDown();
                try{while(true){try{release.await();break;}catch(InterruptedException cancelled){ /* underlying storage ignores cancellation */ }} }
                finally{permit.close();}
            });
            assertTrue(entered.await(2,TimeUnit.SECONDS));
            assertTrue(cancelledCall.cancel(true));
            assertThrows(IOException.class,node::close);
            assertThrows(IOException.class,()->WalDirectoryLease.acquire(dir));
            release.countDown();
            long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(2);
            while(node.operationGateForTest().activeCount()!=0 && System.nanoTime()<until)Thread.onSpinWait();
            assertEquals(0,node.operationGateForTest().activeCount());
            node.close();
        }finally{release.countDown();permit.close();pool.shutdownNow();}
    }
}
