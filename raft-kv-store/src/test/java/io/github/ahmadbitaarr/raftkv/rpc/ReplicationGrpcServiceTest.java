package io.github.ahmadbitaarr.raftkv.rpc;
import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.inprocess.*;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.server.NodeStatusSnapshot;
import io.github.ahmadbitaarr.raftkv.server.NodeLifecycleState;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;
class ReplicationGrpcServiceTest {
 @TempDir Path dir;
 private static class Rig implements AutoCloseable {
  final Server s;final ManagedChannel c; final ReplicationServiceGrpc.ReplicationServiceBlockingStub stub;
  Rig(ReplicationGrpcService service)throws Exception{var name=InProcessServerBuilder.generateName();s=InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start();c=InProcessChannelBuilder.forName(name).directExecutor().build();stub=ReplicationServiceGrpc.newBlockingStub(c).withDeadlineAfter(5,TimeUnit.SECONDS);}
  public void close(){c.shutdownNow();s.shutdownNow();}
 }
 private Rig rig(String id,long last,long commit)throws Exception {var cfg=ClusterConfigTest.config(dir,id);var snap=new NodeStatusSnapshot(id,cfg.localRole(),NodeLifecycleState.CONTROL_ONLY,last,commit,0,false);return new Rig(new ReplicationGrpcService(cfg,()->snap));}
 private ReplicateEntryRequest replicate(){return ReplicateEntryRequest.newBuilder().setLeaderId("n1").setEntry(LogEntry.newBuilder().setIndex(1).setRequestId("request").setOperation(OperationType.OPERATION_TYPE_PUT).setKey("key").setValue(ByteString.EMPTY).setCommandFingerprint(ByteString.copyFromUtf8("x"))).build();}
 private ReplicationStatusRequest status(){return ReplicationStatusRequest.newBuilder().setLeaderId("n1").build();}
 @Test void failedFollowerCannotAdvertiseHealthyReplicationStatus()throws Exception {
  var cfg=ClusterConfigTest.config(dir,"n2");
  var snap=new NodeStatusSnapshot("n2",cfg.localRole(),NodeLifecycleState.FAILED,2,1,1,false);
  try(var r=new Rig(new ReplicationGrpcService(cfg,()->snap))){
   var e=assertThrows(StatusRuntimeException.class,()->r.stub.getReplicationStatus(status()));
   assertEquals(Status.Code.FAILED_PRECONDITION,e.getStatus().getCode());
  }
 }
 @Test void stoppingFollowerCannotAdvertiseHealthyReplicationStatus()throws Exception {
  var cfg=ClusterConfigTest.config(dir,"n2");
  var snap=new NodeStatusSnapshot("n2",cfg.localRole(),NodeLifecycleState.STOPPING,2,1,1,false);
  try(var r=new Rig(new ReplicationGrpcService(cfg,()->snap))){
   var e=assertThrows(StatusRuntimeException.class,()->r.stub.getReplicationStatus(status()));
   assertEquals(Status.Code.FAILED_PRECONDITION,e.getStatus().getCode());
  }
 }
 @Test void getReplicationStatusReportsConfiguredLeaderRole()throws Exception{try(var r=rig("n1",0,0)){assertEquals(NodeRole.NODE_ROLE_LEADER,r.stub.getReplicationStatus(status()).getRole());}}
 @Test void getReplicationStatusReportsFollowerRole()throws Exception{try(var r=rig("n2",0,0)){assertEquals(NodeRole.NODE_ROLE_FOLLOWER,r.stub.getReplicationStatus(status()).getRole());}}
 @Test void statusReportsDurableLogAndCommitButZeroApplied()throws Exception{try(var r=rig("n2",3,2)){var x=r.stub.getReplicationStatus(status());assertEquals(3,x.getLastLogIndex());assertEquals(2,x.getCommitIndex());assertEquals(0,x.getAppliedIndex());}}
 @Test void statusAlwaysReportsReadyFalse()throws Exception{try(var r=rig("n2",0,0)){assertFalse(r.stub.getReplicationStatus(status()).getReady());}}
 @Test void rejectsWrongClaimedLeaderIdWithFailedPrecondition()throws Exception{try(var r=rig("n2",0,0)){var ex=assertThrows(StatusRuntimeException.class,()->r.stub.getReplicationStatus(ReplicationStatusRequest.newBuilder().setLeaderId("n2").build()));assertEquals(Status.Code.FAILED_PRECONDITION,ex.getStatus().getCode());}}
 @Test void followerReplicateEntryRejectsNotReadyWithoutDurableAck()throws Exception{try(var r=rig("n2",0,0)){var x=r.stub.replicateEntry(replicate());assertFalse(x.getAccepted());assertEquals(ReplicationRejectReason.REPLICATION_REJECT_REASON_NOT_READY,x.getRejectReason());}}
 @Test void leaderRejectsReplicationTargetRole()throws Exception{try(var r=rig("n1",0,0)){assertEquals(Status.Code.FAILED_PRECONDITION,assertThrows(StatusRuntimeException.class,()->r.stub.replicateEntry(replicate())).getStatus().getCode());}}
 @Test void advanceCommitNeverPersistsCommit()throws Exception{try(var r=rig("n2",0,0)){assertEquals(Status.Code.FAILED_PRECONDITION,assertThrows(StatusRuntimeException.class,()->r.stub.advanceCommit(AdvanceCommitRequest.newBuilder().setLeaderId("n1").setCommitIndex(3).build())).getStatus().getCode());}}
 @Test void truncateSuffixNeverChangesWal()throws Exception{try(var r=rig("n2",3,0)){assertEquals(Status.Code.FAILED_PRECONDITION,assertThrows(StatusRuntimeException.class,()->r.stub.truncateUncommittedSuffix(TruncateSuffixRequest.newBuilder().setLeaderId("n1").setTruncateAfterIndex(0).build())).getStatus().getCode());}}
 @Test void malformedReplicationRequestReturnsInvalidArgument()throws Exception{try(var r=rig("n2",0,0)){assertEquals(Status.Code.INVALID_ARGUMENT,assertThrows(StatusRuntimeException.class,()->r.stub.replicateEntry(ReplicateEntryRequest.getDefaultInstance())).getStatus().getCode());}}
 @Test void replicationRejectedResponsePassesPhase4Validator()throws Exception{try(var r=rig("n2",0,0)){ReplicationContractValidator.validate(r.stub.replicateEntry(replicate()));}}
 @Test void statusNeverClaimsCommittedEntriesApplied()throws Exception{try(var r=rig("n2",3,2)){assertEquals(0,r.stub.getReplicationStatus(status()).getAppliedIndex());}}
}
