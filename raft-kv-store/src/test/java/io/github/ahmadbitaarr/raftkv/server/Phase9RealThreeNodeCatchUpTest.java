package io.github.ahmadbitaarr.raftkv.server;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.github.ahmadbitaarr.raftkv.config.*;
import io.github.ahmadbitaarr.raftkv.replication.GrpcFollowerClientTestSupport;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.wal.FileWalStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

/** Real Netty communication, distinct WAL owners, OS-assigned ports and automatic idle catch-up. */
class Phase9RealThreeNodeCatchUpTest {
    @TempDir Path root;
    private final Map<String,NodeEndpoint> live=new ConcurrentHashMap<>();
    private ClusterConfig cfg(String id){
        var members=ClusterConfigTest.members();
        return new ClusterConfig("phase9",id,"n1",members,"127.0.0.1",members.get(id).port(),
                root.resolve(id),4000,1200,500,2000,12582912);
    }
    private NodeServer follower(String id){return new NodeServer(cfg(id),0);}
    private NodeServer leader(){
        return new NodeServer(cfg("n1"),0,c -> GrpcFollowerClientTestSupport.withEndpointResolver(c,id -> {
            NodeEndpoint endpoint=live.get(id);
            if(endpoint==null)throw new IllegalArgumentException("offline peer: "+id);
            return endpoint;
        }));
    }
    private void online(String id,NodeServer server){
        assertTrue(server.boundPort()>0);
        live.put(id,new NodeEndpoint("127.0.0.1",server.boundPort()));
    }
    private static ManagedChannel channel(NodeServer node){
        return ManagedChannelBuilder.forAddress("127.0.0.1",node.boundPort()).usePlaintext().build();
    }
    private static KeyValueServiceGrpc.KeyValueServiceBlockingStub client(ManagedChannel channel){
        return KeyValueServiceGrpc.newBlockingStub(channel).withDeadlineAfter(8,TimeUnit.SECONDS);
    }
    private static MutationResponse put(ManagedChannel channel,String id,String key,byte[] value){
        return client(channel).put(PutRequest.newBuilder().setRequestId(id).setKey(key).setValue(ByteString.copyFrom(value)).build());
    }
    private static MutationResponse delete(ManagedChannel channel,String id,String key){
        return client(channel).delete(DeleteRequest.newBuilder().setRequestId(id).setKey(key).build());
    }
    private static GetResponse get(ManagedChannel channel,String key){
        return client(channel).get(GetRequest.newBuilder().setKey(key).build());
    }
    private static void eventually(BooleanSupplier condition,String context) throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(12);
        while(System.nanoTime()<deadline) {
            if(condition.getAsBoolean())return;
            Thread.sleep(20); // bounded polling, not an ordering/proof oracle
        }
        fail("timed out waiting for "+context);
    }

    @Test void stoppedFollowerRestartsAndAutomaticallyReplaysMissedCommitsWithoutNewClientWrites() throws Exception {
        NodeServer n3=null;
        try(var n2=follower("n2");var n1=leader()) {
            n3=follower("n3");
            try {
                n2.start();online("n2",n2);
                n3.start();online("n3",n3);
                n1.start();
                ManagedChannel leaderChannel=channel(n1);
                try {
                    assertEquals(ResultCode.RESULT_CODE_OK,put(leaderChannel,"r1","x",new byte[]{1,0,2}).getCode());
                    assertEquals(ResultCode.RESULT_CODE_OK,put(leaderChannel,"r2","y",new byte[]{8}).getCode());
                    NodeServer firstN3=n3;
                    eventually(()->firstN3.statusSnapshot().durableCommitIndex()==2
                            && firstN3.statusSnapshot().appliedIndex()==2,"initial all-node commit");
                    n3.close();live.remove("n3");n3=null;
                    assertEquals(ResultCode.RESULT_CODE_OK,put(leaderChannel,"r3","x",new byte[]{4,0,6}).getCode());
                    assertEquals(ResultCode.RESULT_CODE_OK,delete(leaderChannel,"r4","y").getCode());
                    assertEquals(4,n1.statusSnapshot().durableCommitIndex());
                    assertEquals(ResultCode.RESULT_CODE_OK,get(leaderChannel,"x").getCode());
                    assertArrayEquals(new byte[]{4,0,6},get(leaderChannel,"x").getValue().toByteArray());
                    assertEquals(ResultCode.RESULT_CODE_NOT_FOUND,get(leaderChannel,"y").getCode());
                    // The same WAL directory survives the restart, but the OS chooses a new test port.
                    n3=follower("n3");n3.start();online("n3",n3);
                    NodeServer returned=n3;
                    assertEquals(2,returned.statusSnapshot().durableCommitIndex());
                    assertEquals(2,returned.statusSnapshot().appliedIndex());
                    eventually(()->returned.statusSnapshot().lastLogIndex()==4
                            && returned.statusSnapshot().durableCommitIndex()==4
                            && returned.statusSnapshot().appliedIndex()==4,
                            "idle automatic catch-up of restored follower");
                    assertArrayEquals(new byte[]{4,0,6},returned.followerApplierForTest()
                            .diagnosticGetForTest("x").orElseThrow());
                    assertTrue(returned.followerApplierForTest().diagnosticGetForTest("y").isEmpty());
                    assertArrayEquals(new byte[]{4,0,6},n2.followerApplierForTest()
                            .diagnosticGetForTest("x").orElseThrow());
                    ManagedChannel n3Channel=channel(returned);
                    try {
                        assertEquals(ResultCode.RESULT_CODE_NOT_LEADER,get(n3Channel,"x").getCode());
                    } finally {n3Channel.shutdownNow();}
                    n2.close();live.remove("n2");
                    assertEquals(ResultCode.RESULT_CODE_OK,put(leaderChannel,"r5","z",new byte[]{7}).getCode());
                    assertEquals(5,n1.statusSnapshot().durableCommitIndex());
                    eventually(()->returned.statusSnapshot().appliedIndex()==5,
                            "new writes after repaired follower replaces healthy peer");
                    assertEquals(5,get(leaderChannel,"z").getCommitIndex());
                } finally {leaderChannel.shutdownNow();}
            } finally {
                if(n3!=null)n3.close();
            }
        }
        try(var wal=FileWalStore.open(cfg("n3").walFile())){
            assertEquals(5,wal.lastDurableLogIndex());
            assertEquals(5,wal.durableCommitIndex());
        }
    }

    @Test void newEmptyFollowerJoinsAlreadyCommittedLeaderWithoutAnyNewClientMutation()throws Exception {
        try(var n2=follower("n2");var n1=leader()) {
            n2.start();online("n2",n2);n1.start();
            var leaderChannel=channel(n1);
            try {
                assertEquals(ResultCode.RESULT_CODE_OK,put(leaderChannel,"pre-join-1","x",new byte[]{1}).getCode());
                assertEquals(ResultCode.RESULT_CODE_OK,put(leaderChannel,"pre-join-2","y",new byte[]{2}).getCode());
                try(var n3=follower("n3")) {
                    n3.start();assertEquals(0,n3.statusSnapshot().lastLogIndex());online("n3",n3);
                    eventually(()->n3.statusSnapshot().lastLogIndex()==2
                            && n3.statusSnapshot().durableCommitIndex()==2
                            && n3.statusSnapshot().appliedIndex()==2,
                            "new empty follower joins idle committed leader");
                    assertArrayEquals(new byte[]{1},n3.followerApplierForTest().diagnosticGetForTest("x").orElseThrow());
                    assertArrayEquals(new byte[]{2},n3.followerApplierForTest().diagnosticGetForTest("y").orElseThrow());
                }
            } finally {leaderChannel.shutdownNow();}
        }
    }

    @Test void followerStartReplaysOnlyDurablyCommittedPrefixAndNeverEnablesClientGet()throws Exception {
        try(var wal=FileWalStore.open(cfg("n2").walFile())){
            var one=new io.github.ahmadbitaarr.raftkv.log.LogEntry(1,"committed",
                    io.github.ahmadbitaarr.raftkv.log.Command.put("x",new byte[]{1}),new byte[]{1});
            var two=new io.github.ahmadbitaarr.raftkv.log.LogEntry(2,"uncommitted",
                    io.github.ahmadbitaarr.raftkv.log.Command.put("x",new byte[]{2}),new byte[]{2});
            wal.appendEntryDurably(one);wal.appendEntryDurably(two);wal.appendCommitDurably(1);
        }
        try(var recovered=follower("n2")){
            recovered.start();assertEquals(NodeLifecycleState.CONTROL_ONLY,recovered.statusSnapshot().state());
            assertEquals(2,recovered.statusSnapshot().lastLogIndex());
            assertEquals(1,recovered.statusSnapshot().durableCommitIndex());
            assertEquals(1,recovered.statusSnapshot().appliedIndex());
            assertArrayEquals(new byte[]{1},recovered.followerApplierForTest().diagnosticGetForTest("x").orElseThrow());
            var channel=channel(recovered);
            try {assertEquals(ResultCode.RESULT_CODE_NOT_LEADER,get(channel,"x").getCode());}
            finally {channel.shutdownNow();}
        }
    }
}
