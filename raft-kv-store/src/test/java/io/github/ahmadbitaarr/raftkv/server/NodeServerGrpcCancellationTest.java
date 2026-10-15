package io.github.ahmadbitaarr.raftkv.server;

import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.github.ahmadbitaarr.raftkv.config.*;
import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.rpc.ProtoMapper;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** A transport deadline does not prove the backing storage operation has finished. */
class NodeServerGrpcCancellationTest {
    @TempDir Path dir;
    @Test void cancelledActualGrpcHandlerCannotRelinquishWalLease()throws Exception {
        var standard=ClusterConfigTest.config(dir,"n2");
        var cfg=new ClusterConfig(standard.clusterId(),standard.localNodeId(),standard.configuredLeaderId(),
                standard.members(),standard.localBindHost(),standard.localBindPort(),dir,100,100,100,50,12582912);
        CountDownLatch inAppend=new CountDownLatch(1),allowAppend=new CountDownLatch(1);
        var node=new NodeServer(cfg,0,(conf,port,client,replication)->
                NettyServerBuilder.forPort(0).maxInboundMessageSize(conf.maxInboundMessageBytes())
                    .addService(client).addService(replication).build(), path->new HeldWal(FileWalStore.open(path),inAppend,allowAppend));
        node.start();
        var channel=ManagedChannelBuilder.forAddress("127.0.0.1",node.boundPort()).usePlaintext().build();
        var request=ReplicateEntryRequest.newBuilder().setLeaderId("n1")
                .setEntry(ProtoMapper.toProtoLogEntry(new LogEntry(1,"r",Command.put("key",new byte[]{8}),new byte[]{9})))
                .build();
        ExecutorService caller=Executors.newSingleThreadExecutor();
        try {
            Future<Status.Code> rpc=caller.submit(()->{
                try {
                    ReplicationServiceGrpc.newBlockingStub(channel).withDeadlineAfter(400,TimeUnit.MILLISECONDS)
                            .replicateEntry(request);
                    return Status.Code.OK;
                }catch(StatusRuntimeException failure){return failure.getStatus().getCode();}
            });
            assertTrue(inAppend.await(3,TimeUnit.SECONDS),"must see real handler enter WAL before cancellation");
            assertEquals(Status.Code.DEADLINE_EXCEEDED,rpc.get(3,TimeUnit.SECONDS));
            assertThrows(IOException.class,node::close);
            assertNotEquals(NodeLifecycleState.STOPPED,node.statusSnapshot().state());
            assertThrows(IOException.class,()->WalDirectoryLease.acquire(dir));
        }finally{
            allowAppend.countDown();channel.shutdownNow();caller.shutdownNow();
            long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
            while(node.operationGateForTest().activeCount()!=0 && System.nanoTime()<until)Thread.onSpinWait();
            node.close();
        }
        try(var lease=WalDirectoryLease.acquire(dir)){assertTrue(lease.isHeld());}
    }
    private static final class HeldWal implements WalStore {
        private final WalStore delegate; private final CountDownLatch entered, release;
        HeldWal(WalStore d,CountDownLatch entered,CountDownLatch release){this.delegate=d;this.entered=entered;this.release=release;}
        public WalRecoveryResult recoveryResult(){return delegate.recoveryResult();}
        public void appendEntryDurably(LogEntry entry)throws IOException {
            entered.countDown();
            // I/O abstraction deliberately remains blocked even if gRPC cancels/interupts its caller.
            while(true){try{release.await();break;}catch(InterruptedException cancelled){} }
            delegate.appendEntryDurably(entry);
        }
        public void appendCommitDurably(long index)throws IOException{delegate.appendCommitDurably(index);}
        public void rewriteDurably(List<LogEntry> entries,long index)throws IOException{delegate.rewriteDurably(entries,index);}
        public long lastDurableLogIndex(){return delegate.lastDurableLogIndex();}
        public long durableCommitIndex(){return delegate.durableCommitIndex();}
        public void close()throws IOException{delegate.close();}
    }
}
