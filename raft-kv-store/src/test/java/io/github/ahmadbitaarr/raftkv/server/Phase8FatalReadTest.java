package io.github.ahmadbitaarr.raftkv.server;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class Phase8FatalReadTest {
    @TempDir Path dir;
    private static final class CorruptingWal implements WalStore {
        final WalStore backing;
        CorruptingWal(WalStore backing){this.backing=backing;}
        public WalRecoveryResult recoveryResult(){return backing.recoveryResult();}
        public void appendEntryDurably(LogEntry e)throws IOException{throw new WalCorruptionException("injected corruption");}
        public void appendCommitDurably(long index)throws IOException{backing.appendCommitDurably(index);}
        public void rewriteDurably(List<LogEntry> entries,long index)throws IOException{backing.rewriteDurably(entries,index);}
        public long lastDurableLogIndex(){return backing.lastDurableLogIndex();}
        public long durableCommitIndex(){return backing.durableCommitIndex();}
        public void close()throws IOException{backing.close();}
    }
    @Test void confirmedStorageFailureDisablesReadAndDoesNotServeStaleData() throws Exception {
        var cfg=ClusterConfigTest.config(dir,"n1");
        var node=new NodeServer(cfg,0,(c,p,client,peer)->NettyServerBuilder.forPort(p)
                .maxInboundMessageSize(c.maxInboundMessageBytes()).addService(client).addService(peer).build(),
                path->new CorruptingWal(FileWalStore.open(path)));
        ManagedChannel channel=null;
        try {
            node.start();assertTrue(node.statusSnapshot().ready());
            channel=ManagedChannelBuilder.forAddress("127.0.0.1",node.boundPort()).usePlaintext().build();
            var client=KeyValueServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5,TimeUnit.SECONDS);
            var put=PutRequest.newBuilder().setRequestId("r1").setKey("x")
                    .setValue(ByteString.copyFrom(new byte[]{1})).build();
            assertEquals(Status.Code.DATA_LOSS,
                    assertThrows(StatusRuntimeException.class,()->client.put(put)).getStatus().getCode());
            assertEquals(NodeLifecycleState.FAILED,node.statusSnapshot().state());
            assertFalse(node.statusSnapshot().ready());
            var read=client.get(GetRequest.newBuilder().setKey("x").build());
            assertEquals(ResultCode.RESULT_CODE_NOT_READY,read.getCode());
            assertFalse(read.hasValue());assertFalse(read.hasCommitIndex());
            assertEquals(0,node.statusSnapshot().durableCommitIndex());
        }finally {
            if(channel!=null)channel.shutdownNow();
            var closeFailure=assertThrows(IOException.class,node::close);
            assertTrue(closeFailure.getMessage().contains("fault"));
        }
    }
}
