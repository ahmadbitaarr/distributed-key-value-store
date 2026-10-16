package io.github.ahmadbitaarr.raftkv.server;
import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.rpc.RpcLimits;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicationServiceGrpc;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicationStatusRequest;
import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.wal.FileWalStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;
class NodeLoopbackGrpcTest {
 @TempDir Path dir;
 private NodeServer node(String id)throws Exception {var n=new NodeServer(ClusterConfigTest.config(dir,id),0);n.start();return n;}
 private ManagedChannel channel(int port){return ManagedChannelBuilder.forAddress("127.0.0.1",port).usePlaintext().maxInboundMessageSize(RpcLimits.CLIENT_INBOUND_MESSAGE_BYTES).build();}
 private KeyValueServiceGrpc.KeyValueServiceBlockingStub client(ManagedChannel c){return KeyValueServiceGrpc.newBlockingStub(c).withDeadlineAfter(15,TimeUnit.SECONDS);}
 private ReplicationServiceGrpc.ReplicationServiceBlockingStub peer(ManagedChannel c){return ReplicationServiceGrpc.newBlockingStub(c).withDeadlineAfter(15,TimeUnit.SECONDS);}
 @Test void nodeBindsRealLoopbackGrpcListener()throws Exception{try(var n=node("n1")){var c=channel(n.boundPort());try{assertEquals(ResultCode.RESULT_CODE_NOT_READY,client(c).get(GetRequest.newBuilder().setKey("x").build()).getCode());}finally{c.shutdownNow();}}}
 @Test void followerLoopbackClientGetReturnsNotLeader()throws Exception{try(var n=node("n2")){var c=channel(n.boundPort());try{assertEquals(ResultCode.RESULT_CODE_NOT_LEADER,client(c).get(GetRequest.newBuilder().setKey("x").build()).getCode());}finally{c.shutdownNow();}}}
 @Test void leaderLoopbackClientGetReturnsNotReadyWithCommittedWal()throws Exception{try(var store=FileWalStore.open(dir.resolve("raftkv.wal"))){store.appendEntryDurably(new LogEntry(1,"r",Command.put("x",new byte[]{1}),new byte[]{2}));store.appendCommitDurably(1);}try(var n=node("n1")){var c=channel(n.boundPort());try{assertEquals(ResultCode.RESULT_CODE_NOT_READY,client(c).get(GetRequest.newBuilder().setKey("x").build()).getCode());}finally{c.shutdownNow();}}}
 @Test void loopbackReplicationStatusIsValidAndNotReady()throws Exception{try(var n=node("n2")){var c=channel(n.boundPort());try{var x=peer(c).getReplicationStatus(ReplicationStatusRequest.newBuilder().setLeaderId("n1").build());assertFalse(x.getReady());assertEquals(0,x.getAppliedIndex());}finally{c.shutdownNow();}}}
 @Test void loopbackLargePutOverFourMiBReachesHandler()throws Exception{
  try(var n=node("n1")){
   var c=channel(n.boundPort());
   try{
    var request=PutRequest.newBuilder().setRequestId("r").setKey("k")
     .setValue(ByteString.copyFrom(new byte[5*1024*1024])).build();
    // The 5 MiB request reaches Phase 7's mutation handler; no follower is running.
    var response=client(c).put(request);
    assertEquals(ResultCode.RESULT_CODE_NO_QUORUM,response.getCode());
    assertEquals("r",response.getRequestId());
    assertFalse(response.hasCommitIndex());
   }finally{c.shutdownNow();}
  }
 }
 @Test void loopbackPutOverApplicationEightMiBLimitIsInvalidArgument()throws Exception{try(var n=node("n1")){var c=channel(n.boundPort());try{var request=PutRequest.newBuilder().setRequestId("r").setKey("k").setValue(ByteString.copyFrom(new byte[RpcLimits.MAX_VALUE_BYTES+1])).build();assertEquals(Status.Code.INVALID_ARGUMENT,assertThrows(StatusRuntimeException.class,()->client(c).put(request)).getStatus().getCode());}finally{c.shutdownNow();}}}
 @Test void loopbackMessageOverTwelveMiBRejectedAtTransport()throws Exception{try(var n=node("n1")){var c=channel(n.boundPort());try{var request=PutRequest.newBuilder().setRequestId("r").setKey("k").setValue(ByteString.copyFrom(new byte[RpcLimits.MAX_SERIALIZED_MESSAGE_BYTES+1024])).build();var ex=assertThrows(StatusRuntimeException.class,()->client(c).put(request));assertEquals(Status.Code.RESOURCE_EXHAUSTED,ex.getStatus().getCode());}finally{c.shutdownNow();}}}
 @Test void loopbackTestOnlyLargeResponseHonorsClientInboundLimit()throws Exception{
  Server s=io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder.forPort(0).maxInboundMessageSize(RpcLimits.SERVER_INBOUND_MESSAGE_BYTES)
   .addService(new KeyValueServiceGrpc.KeyValueServiceImplBase(){@Override public void get(GetRequest r,StreamObserver<GetResponse> o){o.onNext(GetResponse.newBuilder().setCode(ResultCode.RESULT_CODE_OK).setKey(r.getKey()).setValue(ByteString.copyFrom(new byte[8*1024*1024])).setCommitIndex(0).build());o.onCompleted();}}).build().start();
  var c=channel(s.getPort());try{assertEquals(8*1024*1024,client(c).get(GetRequest.newBuilder().setKey("k").build()).getValue().size());}finally{c.shutdownNow();s.shutdownNow();s.awaitTermination(5,TimeUnit.SECONDS);}
 }
 @Test void stoppedServerRejectsNewCalls()throws Exception{var n=node("n1");int p=n.boundPort();n.close();var c=channel(p);try{assertThrows(StatusRuntimeException.class,()->client(c).get(GetRequest.newBuilder().setKey("k").build()));}finally{c.shutdownNow();}}
 @Test void bindConflictFailsWithoutDamagingOriginalWal()throws Exception{try(var n=node("n1")){try(var dup=new NodeServer(ClusterConfigTest.config(dir,"n1"),n.boundPort())){assertThrows(Exception.class,dup::start);}}}
 @Test void reopenAfterShutdownPreservesRecoveredWalMetadata()throws Exception{try(var store=FileWalStore.open(dir.resolve("raftkv.wal"))){store.appendEntryDurably(new LogEntry(1,"r",Command.delete("x"),new byte[]{2}));}try(var n=node("n1")){assertEquals(1,n.statusSnapshot().lastLogIndex());}try(var n=node("n1")){assertEquals(1,n.statusSnapshot().lastLogIndex());}}
}
