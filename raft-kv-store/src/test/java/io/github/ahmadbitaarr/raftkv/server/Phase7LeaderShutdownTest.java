package io.github.ahmadbitaarr.raftkv.server;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.github.ahmadbitaarr.raftkv.config.*;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

class Phase7LeaderShutdownTest {
    @TempDir Path dir;
    private static final class BlockEntryWal implements WalStore {
        final WalStore delegate;
        final CountDownLatch entered=new CountDownLatch(1), release=new CountDownLatch(1);
        BlockEntryWal(WalStore delegate){this.delegate=delegate;}
        @Override public WalRecoveryResult recoveryResult(){return delegate.recoveryResult();}
        @Override public void appendEntryDurably(LogEntry entry)throws IOException {
            entered.countDown();
            while(true){try {release.await();break;}catch(InterruptedException cancelled){ /* model non-interruptible disk */ }}
            delegate.appendEntryDurably(entry);
        }
        @Override public void appendCommitDurably(long n)throws IOException{delegate.appendCommitDurably(n);}
        @Override public void rewriteDurably(List<LogEntry> entries,long n)throws IOException{delegate.rewriteDurably(entries,n);}
        @Override public long lastDurableLogIndex(){return delegate.lastDurableLogIndex();}
        @Override public long durableCommitIndex(){return delegate.durableCommitIndex();}
        @Override public void close()throws IOException{delegate.close();}
    }
    private ClusterConfig config(int timeout) {
        var base=ClusterConfigTest.config(dir,"n1");
        return new ClusterConfig(base.clusterId(),base.localNodeId(),base.configuredLeaderId(),base.members(),
                base.localBindHost(),base.localBindPort(),base.walDirectory(),
                2000,200,100,timeout,12582912);
    }
    @Test void shutdownTimeoutPreservesWalLeaseDuringBlockedLeaderForceAndCancelledCall()throws Exception {
        var config=config(40);
        // Open through NodeServer only after its pre-open WAL provenance check.
        // Pre-opening here would make a fresh WAL look like recovery history.
        var openedStore=new AtomicReference<BlockEntryWal>();
        var node=new NodeServer(config,0,(cfg,port,client,replication)->NettyServerBuilder
                .forPort(port).maxInboundMessageSize(cfg.maxInboundMessageBytes())
                .addService(client).addService(replication).build(),path->{
                    var store=new BlockEntryWal(FileWalStore.open(path));
                    openedStore.set(store);
                    return store;
                });
        ExecutorService pool=Executors.newSingleThreadExecutor();
        ManagedChannel channel=null;
        try {
            node.start();
            var store=Objects.requireNonNull(openedStore.get(),"WAL opener not invoked");
            assertEquals(NodeLifecycleState.READY,node.statusSnapshot().state());
            channel=ManagedChannelBuilder.forAddress("127.0.0.1",node.boundPort()).usePlaintext().build();
            ManagedChannel actualChannel=channel;
            Future<?> call=pool.submit(()->KeyValueServiceGrpc.newBlockingStub(actualChannel)
                    .withDeadlineAfter(5,TimeUnit.SECONDS).put(PutRequest.newBuilder()
                            .setRequestId("blocked").setKey("a").setValue(ByteString.copyFrom(new byte[]{1})).build()));
            assertTrue(store.entered.await(3,TimeUnit.SECONDS));
            assertTrue(call.cancel(true));
            assertThrows(IOException.class,node::close);
            assertEquals(NodeLifecycleState.STOPPING,node.statusSnapshot().state());
            assertThrows(IOException.class,()->WalDirectoryLease.acquire(dir));
            store.release.countDown();
            long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);
            while(node.statusSnapshot().lastLogIndex()==0 && System.nanoTime()<until)Thread.onSpinWait();
            assertEquals(1,node.statusSnapshot().lastLogIndex());
            // The application handler can outlive a cancelled/terminated transport.
            long retryUntil=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);
            IOException last=null;
            do {
                try {node.close();last=null;break;}
                catch(IOException busy) {last=busy;Thread.yield();}
            }while(System.nanoTime()<retryUntil);
            if(last!=null)throw last;
            try(var lease=WalDirectoryLease.acquire(dir)){assertTrue(lease.isHeld());}
        }finally {
            var store=openedStore.get();
            if(store!=null)store.release.countDown();
            if(channel!=null)channel.shutdownNow();
            pool.shutdownNow();
            if(node.statusSnapshot().state()!=NodeLifecycleState.STOPPED)try{node.close();}catch(IOException ignored){}
        }
    }
}
