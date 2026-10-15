package io.github.ahmadbitaarr.raftkv.rpc;
import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.inprocess.*;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.server.*;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;
class ClientKvGrpcServiceTest {
 @TempDir Path dir;
 private static class Rig implements AutoCloseable{
  final Server s; final ManagedChannel c; final KeyValueServiceGrpc.KeyValueServiceBlockingStub stub;
  Rig(ClientKvGrpcService handler)throws Exception{String name=InProcessServerBuilder.generateName();s=InProcessServerBuilder.forName(name).directExecutor().addService(handler).build().start();c=InProcessChannelBuilder.forName(name).directExecutor().build();stub=KeyValueServiceGrpc.newBlockingStub(c).withDeadlineAfter(5,TimeUnit.SECONDS);}
  public void close(){c.shutdownNow();s.shutdownNow();}
 }
 private Rig rig(String id)throws Exception {var config=ClusterConfigTest.config(dir,id);NodeStatusSnapshot snapshot=new NodeStatusSnapshot(id,config.localRole(),NodeLifecycleState.CONTROL_ONLY,0,0,0,false);return new Rig(new ClientKvGrpcService(config,()->snapshot));}
 private PutRequest put(){return PutRequest.newBuilder().setRequestId("req-123").setKey("key").setValue(ByteString.EMPTY).build();}
 private DeleteRequest delete(){return DeleteRequest.newBuilder().setRequestId("req-123").setKey("key").build();}
 private GetRequest get(){return GetRequest.newBuilder().setKey("key").build();}
 @Test void followerPutReturnsNotLeaderWithExactRequestIdAndHint()throws Exception{try(var r=rig("n2")){var x=r.stub.put(put());assertEquals(ResultCode.RESULT_CODE_NOT_LEADER,x.getCode());assertEquals("n1",x.getLeaderHint().getLeaderId());ClientContractValidator.validate(x,put());}}
 @Test void followerDeleteReturnsNotLeaderWithExactRequestIdAndHint()throws Exception{try(var r=rig("n2")){var x=r.stub.delete(delete());assertEquals(ResultCode.RESULT_CODE_NOT_LEADER,x.getCode());ClientContractValidator.validate(x,delete());}}
 @Test void followerGetReturnsNotLeaderWithExactKeyAndHint()throws Exception{try(var r=rig("n2")){var x=r.stub.get(get());assertEquals(ResultCode.RESULT_CODE_NOT_LEADER,x.getCode());ClientContractValidator.validate(x,get());}}
 @Test void configuredLeaderPutReturnsNotReadyWithoutCommitIndex()throws Exception{try(var r=rig("n1")){var x=r.stub.put(put());assertEquals(ResultCode.RESULT_CODE_NOT_READY,x.getCode());assertFalse(x.hasCommitIndex());}}
 @Test void configuredLeaderDeleteReturnsNotReadyWithoutCommitIndex()throws Exception{try(var r=rig("n1")){assertEquals(ResultCode.RESULT_CODE_NOT_READY,r.stub.delete(delete()).getCode());}}
 @Test void configuredLeaderGetReturnsNotReadyWithoutValueOrIndex()throws Exception{try(var r=rig("n1")){var x=r.stub.get(get());assertEquals(ResultCode.RESULT_CODE_NOT_READY,x.getCode());assertFalse(x.hasValue());assertFalse(x.hasCommitIndex());}}
 @Test void invalidPutMissingValueRejectedBeforeRoleCheck()throws Exception{try(var r=rig("n2")){var x=assertThrows(StatusRuntimeException.class,()->r.stub.put(PutRequest.newBuilder().setRequestId("r").setKey("k").build()));assertEquals(Status.Code.INVALID_ARGUMENT,x.getStatus().getCode());}}
 @Test void explicitEmptyPutValueIsValidButNotExecuted()throws Exception{try(var r=rig("n1")){assertEquals(ResultCode.RESULT_CODE_NOT_READY,r.stub.put(put()).getCode());}}
 @Test void invalidKeyOrRequestIdReturnsInvalidArgument()throws Exception{try(var r=rig("n1")){assertEquals(Status.Code.INVALID_ARGUMENT,assertThrows(StatusRuntimeException.class,()->r.stub.get(GetRequest.getDefaultInstance())).getStatus().getCode());}}
 @Test void clientResponsesPassPhase4IdentityValidators()throws Exception{try(var r=rig("n1")){ClientContractValidator.validate(r.stub.put(put()),put());ClientContractValidator.validate(r.stub.delete(delete()),delete());ClientContractValidator.validate(r.stub.get(get()),get());}}
 @Test void leaderReadNeverReturnsFoundOrNotFound()throws Exception{try(var r=rig("n1")){assertEquals(ResultCode.RESULT_CODE_NOT_READY,r.stub.get(get()).getCode());}}
 @Test void noRpcClientMethodTouchesWalOrStateMachine()throws Exception{try(var r=rig("n1")){r.stub.put(put());r.stub.delete(delete());r.stub.get(get());assertFalse(java.nio.file.Files.exists(dir.resolve("raftkv.wal")));}}
}
