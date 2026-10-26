package io.github.ahmadbitaarr.raftkv.server;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.github.ahmadbitaarr.raftkv.config.*;
import io.github.ahmadbitaarr.raftkv.replication.GrpcFollowerClientTestSupport;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import io.github.ahmadbitaarr.raftkv.wal.FileWalStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** Real Netty endpoints, distinct WAL owners, actual port-zero bindings; no fixed-port races. */
class Phase8RealThreeNodeTest {
    @TempDir Path root;
    private ClusterConfig config(String id) {
        var members=ClusterConfigTest.members();
        return new ClusterConfig("phase8",id,"n1",members,"127.0.0.1",members.get(id).port(),
                root.resolve(id),4000,1500,1000,2000,12582912);
    }
    private static ManagedChannel channel(NodeServer node) {
        assertTrue(node.boundPort()>0);
        return ManagedChannelBuilder.forAddress("127.0.0.1",node.boundPort()).usePlaintext().build();
    }
    private static void register(Map<String,NodeEndpoint> live,String id,NodeServer server){
        live.put(id,new NodeEndpoint("127.0.0.1",server.boundPort()));
    }
    private static NodeServer follower(ClusterConfig cfg){return new NodeServer(cfg,0);}
    private static NodeServer leader(ClusterConfig cfg,Map<String,NodeEndpoint> live){
        return new NodeServer(cfg,0,c -> GrpcFollowerClientTestSupport.withEndpointResolver(c,id -> {
            var ep=live.get(id);if(ep==null)throw new IllegalArgumentException("not running: "+id);return ep;
        }));
    }
    private static KeyValueServiceGrpc.KeyValueServiceBlockingStub client(ManagedChannel c){
        return KeyValueServiceGrpc.newBlockingStub(c).withDeadlineAfter(8,TimeUnit.SECONDS);
    }
    private static MutationResponse put(ManagedChannel channel,String id,String key,byte[] bytes) {
        return client(channel).put(PutRequest.newBuilder().setRequestId(id).setKey(key)
                .setValue(ByteString.copyFrom(bytes)).build());
    }
    private static MutationResponse delete(ManagedChannel channel,String id,String key) {
        return client(channel).delete(DeleteRequest.newBuilder().setRequestId(id).setKey(key).build());
    }
    private static GetResponse get(ManagedChannel channel,String key){
        return client(channel).get(GetRequest.newBuilder().setKey(key).build());
    }

    @Test void freshLeaderReturnsNotFoundAtIndexZeroWithoutFollowerQuorum() throws Exception {
        try(var n=leader(config("n1"),Map.of())){
            n.start();assertEquals(NodeLifecycleState.READY,n.statusSnapshot().state());
            assertTrue(n.statusSnapshot().ready());
            var c=channel(n);
            try {
                var response=get(c,"missing");
                assertEquals(ResultCode.RESULT_CODE_NOT_FOUND,response.getCode());
                assertTrue(response.hasCommitIndex());assertEquals(0,response.getCommitIndex());
                assertFalse(response.hasValue());
                var status=ReplicationServiceGrpc.newBlockingStub(c)
                        .getReplicationStatus(ReplicationStatusRequest.newBuilder().setLeaderId("n1").build());
                assertTrue(status.getReady());assertEquals(0,status.getCommitIndex());
            } finally {c.shutdownNow();}
        }
    }
    @Test void threeNodePutOverwriteEmptyBinaryAndDeleteHaveCoherentReadIndexes() throws Exception {
        var live=new HashMap<String,NodeEndpoint>();
        try(var n2=follower(config("n2"));var n3=follower(config("n3"));var n1=leader(config("n1"),live)){
            n2.start();register(live,"n2",n2);n3.start();register(live,"n3",n3);n1.start();
            var c=channel(n1);var f=channel(n2);
            try {
                assertEquals(ResultCode.RESULT_CODE_OK,put(c,"write-1","x",new byte[]{5,0,7}).getCode());
                var first=get(c,"x");
                assertEquals(ResultCode.RESULT_CODE_OK,first.getCode());
                assertArrayEquals(new byte[]{5,0,7},first.getValue().toByteArray());
                assertEquals(1,first.getCommitIndex());
                assertEquals(ResultCode.RESULT_CODE_OK,put(c,"write-2","x",new byte[0]).getCode());
                var empty=get(c,"x");
                assertEquals(ResultCode.RESULT_CODE_OK,empty.getCode());
                assertTrue(empty.hasValue());assertEquals(0,empty.getValue().size());
                assertEquals(2,empty.getCommitIndex());
                assertEquals(ResultCode.RESULT_CODE_OK,delete(c,"delete-3","x").getCode());
                var deleted=get(c,"x");
                assertEquals(ResultCode.RESULT_CODE_NOT_FOUND,deleted.getCode());
                assertEquals(3,deleted.getCommitIndex());assertFalse(deleted.hasValue());
                assertEquals(ResultCode.RESULT_CODE_NOT_FOUND,get(c,"never-created").getCode());
                assertEquals(ResultCode.RESULT_CODE_OK,delete(c,"delete-4","missing").getCode());
                assertEquals(4,get(c,"missing").getCommitIndex());
                assertEquals(4,n1.statusSnapshot().durableCommitIndex());
                assertEquals(4,n1.statusSnapshot().appliedIndex());
                assertEquals(ResultCode.RESULT_CODE_NOT_LEADER,get(f,"x").getCode());
                assertEquals("n1",get(f,"x").getLeaderHint().getLeaderId());
                var status=ReplicationServiceGrpc.newBlockingStub(f)
                        .getReplicationStatus(ReplicationStatusRequest.newBuilder().setLeaderId("n1").build());
                assertFalse(status.getReady());
            }finally{f.shutdownNow();c.shutdownNow();}
        }
    }
    @Test void oneFollowerSufficesForWriteAndRead() throws Exception {
        var live=new HashMap<String,NodeEndpoint>();
        try(var n2=follower(config("n2"));var n1=leader(config("n1"),live)) {
            n2.start();register(live,"n2",n2);n1.start();var c=channel(n1);
            try {
                assertEquals(ResultCode.RESULT_CODE_OK,put(c,"id-1","x",new byte[]{1}).getCode());
                assertArrayEquals(new byte[]{1},get(c,"x").getValue().toByteArray());
                assertEquals(1,get(c,"x").getCommitIndex());
            }finally{c.shutdownNow();}
        }
    }
    @Test void unresolvedSecondWriteDoesNotObscureLastCommittedValue() throws Exception {
        var live=new HashMap<String,NodeEndpoint>();var cfg=config("n1");
        try(var n2=follower(config("n2"));var n1=leader(cfg,live)){
            n2.start();register(live,"n2",n2);n1.start();var c=channel(n1);
            try {
                assertEquals(ResultCode.RESULT_CODE_OK,put(c,"id-1","x",new byte[]{1}).getCode());
                n2.close();live.remove("n2");
                assertEquals(ResultCode.RESULT_CODE_NO_QUORUM,put(c,"id-2","x",new byte[]{2}).getCode());
                var committed=get(c,"x");
                assertEquals(ResultCode.RESULT_CODE_OK,committed.getCode());
                assertArrayEquals(new byte[]{1},committed.getValue().toByteArray());
                assertEquals(1,committed.getCommitIndex());
                assertEquals(ResultCode.RESULT_CODE_NOT_READY,put(c,"blocked","x",new byte[]{3}).getCode());
                assertArrayEquals(new byte[]{1},get(c,"x").getValue().toByteArray());
                assertEquals(2,n1.statusSnapshot().lastLogIndex());
                assertEquals(1,n1.statusSnapshot().durableCommitIndex());
                assertEquals(1,n1.statusSnapshot().appliedIndex());
            } finally {c.shutdownNow();}
        }
        try(var recovered=FileWalStore.open(cfg.walFile())) {
            assertEquals(2,recovered.lastDurableLogIndex());assertEquals(1,recovered.durableCommitIndex());
        }
    }
    @Test void repeatedReadsDoNotChangeWalOrReplicationIndexes() throws Exception {
        var live=new HashMap<String,NodeEndpoint>();
        try(var n2=follower(config("n2"));var n1=leader(config("n1"),live)){
            n2.start();register(live,"n2",n2);n1.start();var c=channel(n1);
            try {
                assertEquals(ResultCode.RESULT_CODE_OK,put(c,"id","x",new byte[]{3}).getCode());
                long size=Files.size(config("n1").walFile());
                var before=n1.statusSnapshot();
                for(int i=0;i<20;i++){
                    assertEquals(ResultCode.RESULT_CODE_OK,get(c,"x").getCode());
                    assertEquals(ResultCode.RESULT_CODE_NOT_FOUND,get(c,"absent").getCode());
                }
                assertEquals(size,Files.size(config("n1").walFile()));
                assertEquals(before,n1.statusSnapshot());
            }finally{c.shutdownNow();}
        }
    }
    @Test void concurrentOverwriteAndReadsNeverMixValueWithCommitIndex() throws Exception {
        var live=new HashMap<String,NodeEndpoint>();
        try(var n2=follower(config("n2"));var n1=leader(config("n1"),live)){
            n2.start();register(live,"n2",n2);n1.start();var c=channel(n1);
            ExecutorService workers=Executors.newFixedThreadPool(2);
            try {
                assertEquals(ResultCode.RESULT_CODE_OK,put(c,"initial","x",new byte[]{1}).getCode());
                CountDownLatch start=new CountDownLatch(1);
                Future<MutationResponse> writer=workers.submit(()->{
                    start.await();return put(c,"overwrite","x",new byte[]{2});
                });
                Future<?> reader=workers.submit(()->{
                    start.await();
                    for(int i=0;i<30;i++){
                        var read=get(c,"x");
                        if(read.getCode()==ResultCode.RESULT_CODE_NOT_READY)continue;
                        assertEquals(ResultCode.RESULT_CODE_OK,read.getCode());
                        assertEquals(1,read.getValue().size());
                        assertTrue(read.getCommitIndex()==1 || read.getCommitIndex()==2);
                        assertEquals((byte)read.getCommitIndex(),read.getValue().byteAt(0),
                                "value and commit index must be one logical snapshot");
                    }
                    return null;
                });
                start.countDown();
                assertEquals(ResultCode.RESULT_CODE_OK,writer.get(10,TimeUnit.SECONDS).getCode());
                reader.get(10,TimeUnit.SECONDS);
                var after=get(c,"x");
                assertEquals(ResultCode.RESULT_CODE_OK,after.getCode());
                assertEquals(2,after.getCommitIndex());
                assertArrayEquals(new byte[]{2},after.getValue().toByteArray());
            }finally{workers.shutdownNow();assertTrue(workers.awaitTermination(5,TimeUnit.SECONDS));c.shutdownNow();}
        }
    }
    @Test void recoveredLeaderWithCommitMustNotServeUnreplayedValues() throws Exception {
        var live=new HashMap<String,NodeEndpoint>();var cfg=config("n1");
        try(var n2=follower(config("n2"));var n1=leader(cfg,live)){
            n2.start();register(live,"n2",n2);n1.start();var c=channel(n1);
            try{assertEquals(ResultCode.RESULT_CODE_OK,put(c,"id","x",new byte[]{8}).getCode());}
            finally{c.shutdownNow();}
        }
        try(var restarted=leader(cfg,Map.of())){
            restarted.start();assertEquals(NodeLifecycleState.CONTROL_ONLY,restarted.statusSnapshot().state());
            assertFalse(restarted.statusSnapshot().ready());
            var c=channel(restarted);
            try {
                assertEquals(ResultCode.RESULT_CODE_NOT_READY,get(c,"x").getCode());
                assertEquals(ResultCode.RESULT_CODE_NOT_READY,put(c,"new","y",new byte[]{1}).getCode());
            }finally{c.shutdownNow();}
        }
    }
    @Test void shutdownCannotReturnStaleGetDataAndInvalidGetIsRejected() throws Exception {
        try(var n=leader(config("n1"),Map.of())){
            n.start();var c=channel(n);
            try {
                assertEquals(Status.Code.INVALID_ARGUMENT,assertThrows(StatusRuntimeException.class,
                        ()->get(c,"")).getStatus().getCode());
                n.close();
                assertFalse(n.statusSnapshot().ready());
                assertThrows(StatusRuntimeException.class,()->get(c,"x"));
            }finally{c.shutdownNow();}
        }
    }
}
