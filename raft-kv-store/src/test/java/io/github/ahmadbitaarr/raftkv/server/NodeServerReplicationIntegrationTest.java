package io.github.ahmadbitaarr.raftkv.server;

import io.grpc.*;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.log.WalBackedPersistentLog;
import io.github.ahmadbitaarr.raftkv.rpc.ProtoMapper;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class NodeServerReplicationIntegrationTest {
    @TempDir Path dir;
    private static LogEntry entry(){return new LogEntry(1,"identifier",Command.put("binary",new byte[]{0,127,-1}),new byte[]{9});}
    private ReplicateEntryRequest req(String leader){return ReplicateEntryRequest.newBuilder().setLeaderId(leader).setEntry(ProtoMapper.toProtoLogEntry(entry())).build();}

    @Test void nodeServerRealLoopbackAcceptsAndRecoversFollowerEntry()throws Exception {
        try(var follower=new NodeServer(ClusterConfigTest.config(dir,"n2"),0)){
            follower.start();
            var channel=ManagedChannelBuilder.forAddress("127.0.0.1",follower.boundPort()).usePlaintext().build();
            try {
                var peer=ReplicationServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5,TimeUnit.SECONDS);
                assertTrue(peer.replicateEntry(req("n1")).getAccepted());
                assertEquals(1,peer.getReplicationStatus(ReplicationStatusRequest.newBuilder().setLeaderId("n1").build()).getLastLogIndex());
                assertFalse(follower.statusSnapshot().ready());
                assertEquals(0,follower.statusSnapshot().durableCommitIndex());
                assertTrue(peer.replicateEntry(req("n1")).getAccepted());
            }finally{channel.shutdownNow();}
        }
        try(var recovered=WalBackedPersistentLog.open(dir.resolve("raftkv.wal"))){
            assertEquals(entry(),recovered.get(1).orElseThrow());
        }
    }
    @Test void wrongLeaderAndWrongRoleRemainPreconditionFailures()throws Exception {
        try(var follower=new NodeServer(ClusterConfigTest.config(dir.resolve("follower"),"n2"),0);
            var leader=new NodeServer(ClusterConfigTest.config(dir.resolve("leader"),"n1"),0)){
            follower.start();leader.start();
            var ch1=ManagedChannelBuilder.forAddress("127.0.0.1",follower.boundPort()).usePlaintext().build();
            var ch2=ManagedChannelBuilder.forAddress("127.0.0.1",leader.boundPort()).usePlaintext().build();
            try {
                var p1=ReplicationServiceGrpc.newBlockingStub(ch1).withDeadlineAfter(5,TimeUnit.SECONDS);
                var p2=ReplicationServiceGrpc.newBlockingStub(ch2).withDeadlineAfter(5,TimeUnit.SECONDS);
                assertEquals(Status.Code.FAILED_PRECONDITION,
                    assertThrows(StatusRuntimeException.class,()->p1.replicateEntry(req("n3"))).getStatus().getCode());
                assertEquals(Status.Code.FAILED_PRECONDITION,
                    assertThrows(StatusRuntimeException.class,()->p2.replicateEntry(req("n1"))).getStatus().getCode());
            }finally{ch1.shutdownNow();ch2.shutdownNow();}
        }
    }
}
