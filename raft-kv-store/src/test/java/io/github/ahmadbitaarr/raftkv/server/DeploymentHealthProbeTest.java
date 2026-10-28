package io.github.ahmadbitaarr.raftkv.server;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DeploymentHealthProbeTest {
    private static GetResponse get(ResultCode code) {
        var b=GetResponse.newBuilder().setKey(DeploymentHealthProbe.RESERVED_KEY).setCode(code);
        if(code==ResultCode.RESULT_CODE_OK)b.setValue(ByteString.EMPTY).setCommitIndex(0);
        if(code==ResultCode.RESULT_CODE_NOT_FOUND)b.setCommitIndex(0);
        if(code==ResultCode.RESULT_CODE_NOT_LEADER)
            b.setLeaderHint(LeaderHint.newBuilder().setLeaderId("n1").setHost("node-1").setPort(50051));
        return b.build();
    }
    private static ReplicationStatusResponse follower(String id,long last,long commit,long applied,boolean ready) {
        return ReplicationStatusResponse.newBuilder().setNodeId(id)
                .setRole(io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.NodeRole.NODE_ROLE_FOLLOWER)
                .setLastLogIndex(last).setCommitIndex(commit).setAppliedIndex(applied).setReady(ready).build();
    }
    @Test void validLeaderNotFoundAndPresentEmptyValuesCountAsReadyReads() {
        assertTrue(DeploymentHealthProbe.leaderReady(get(ResultCode.RESULT_CODE_NOT_FOUND)));
        assertTrue(DeploymentHealthProbe.leaderReady(get(ResultCode.RESULT_CODE_OK)));
        assertFalse(DeploymentHealthProbe.leaderReady(GetResponse.newBuilder()
                .setCode(ResultCode.RESULT_CODE_OK).setKey(DeploymentHealthProbe.RESERVED_KEY).build()));
    }
    @Test void leaderProbeNeverMistakesNotReadyOrStaleFollowerForReadiness() {
        assertFalse(DeploymentHealthProbe.leaderReady(get(ResultCode.RESULT_CODE_NOT_READY)));
        assertFalse(DeploymentHealthProbe.leaderReady(get(ResultCode.RESULT_CODE_NOT_LEADER)));
        assertFalse(DeploymentHealthProbe.leaderReady(null));
    }
    @Test void followerControlPlaneAllowsDurableLagButRequiresCorrectIdentity() {
        assertTrue(DeploymentHealthProbe.followerHealthy(follower("n2",9,7,4,false),"n2"));
        assertFalse(DeploymentHealthProbe.followerHealthy(follower("n2",9,7,4,true),"n2"));
        assertFalse(DeploymentHealthProbe.followerHealthy(follower("n2",9,7,4,false),"n3"));
        assertFalse(DeploymentHealthProbe.followerHealthy(follower("n2",2,3,1,false),"n2"));
    }
    @Test void probeArgumentValidationRejectsLeaderFollowerConfusion() {
        assertEquals("follower",DeploymentHealthProbe.parse(new String[] {
                "--role=follower","--node=n2","--leader=n1","--port=50051"}).role());
        assertThrows(IllegalArgumentException.class,()->DeploymentHealthProbe.parse(new String[]{
                "--role=leader","--node=n2","--leader=n1","--port=50051"}));
        assertThrows(IllegalArgumentException.class,()->DeploymentHealthProbe.parse(new String[]{"--role=follower"}));
    }
    @Test void actualLoopbackGrpcProbeHonorsRoleAndStatus() throws Exception {
        Server leader=NettyServerBuilder.forPort(0).addService(new KeyValueServiceGrpc.KeyValueServiceImplBase(){
            @Override public void get(GetRequest r, StreamObserver<GetResponse> observer){
                observer.onNext(GetResponse.newBuilder().setKey(r.getKey()).setCode(ResultCode.RESULT_CODE_NOT_FOUND).setCommitIndex(0).build());
                observer.onCompleted();
            }
        }).build().start();
        try {assertTrue(DeploymentHealthProbe.probe(new DeploymentHealthProbe.Options("leader","n1","n1",leader.getPort())));}
        finally {leader.shutdownNow();leader.awaitTermination();}
        Server follower=NettyServerBuilder.forPort(0).addService(new ReplicationServiceGrpc.ReplicationServiceImplBase(){
            @Override public void getReplicationStatus(ReplicationStatusRequest r,StreamObserver<ReplicationStatusResponse> observer){
                observer.onNext(follower("n2",3,2,1,false));observer.onCompleted();
            }
        }).build().start();
        try {
            assertTrue(DeploymentHealthProbe.probe(new DeploymentHealthProbe.Options("follower","n2","n1",follower.getPort())));
            assertFalse(DeploymentHealthProbe.probe(new DeploymentHealthProbe.Options("follower","n3","n1",follower.getPort())));
        } finally {follower.shutdownNow();follower.awaitTermination();}
    }
}
