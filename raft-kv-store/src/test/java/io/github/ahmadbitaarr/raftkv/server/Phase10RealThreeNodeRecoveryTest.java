package io.github.ahmadbitaarr.raftkv.server;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.github.ahmadbitaarr.raftkv.config.*;
import io.github.ahmadbitaarr.raftkv.leader.CanonicalCommandFingerprint;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.replication.GrpcFollowerClientTestSupport;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.wal.FileWalStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

/** Actual Netty RPC, one persistent WAL per node, OS-assigned ephemeral ports. */
class Phase10RealThreeNodeRecoveryTest {
    @TempDir Path root;
    private final Map<String,NodeEndpoint> live=new ConcurrentHashMap<>();
    private ClusterConfig cfg(String id){
        var members=ClusterConfigTest.members();
        return new ClusterConfig("phase10",id,"n1",members,"127.0.0.1",members.get(id).port(),
                root.resolve(id),4000,1500,600,2000,12582912);
    }
    private NodeServer follower(String id){return new NodeServer(cfg(id),0);}
    private NodeServer leader(){return new NodeServer(cfg("n1"),0,c ->
            GrpcFollowerClientTestSupport.withEndpointResolver(c,id -> {
                var ep=live.get(id);
                if(ep==null)throw new IllegalArgumentException("offline: "+id);
                return ep;
            }));}
    private void online(String id,NodeServer n){
        assertTrue(n.boundPort()>0);
        live.put(id,new NodeEndpoint("127.0.0.1",n.boundPort()));
    }
    private static ManagedChannel channel(NodeServer n){
        return ManagedChannelBuilder.forAddress("127.0.0.1",n.boundPort()).usePlaintext().build();
    }
    private static KeyValueServiceGrpc.KeyValueServiceBlockingStub stub(ManagedChannel c){
        return KeyValueServiceGrpc.newBlockingStub(c).withDeadlineAfter(8,TimeUnit.SECONDS);
    }
    private static MutationResponse put(ManagedChannel c,String id,String key,byte[] data){
        return stub(c).put(PutRequest.newBuilder().setRequestId(id).setKey(key)
                .setValue(ByteString.copyFrom(data)).build());
    }
    private static MutationResponse delete(ManagedChannel c,String id,String key){
        return stub(c).delete(DeleteRequest.newBuilder().setRequestId(id).setKey(key).build());
    }
    private static GetResponse get(ManagedChannel c,String key){
        return stub(c).get(GetRequest.newBuilder().setKey(key).build());
    }
    private static void eventually(BooleanSupplier condition,String description)throws Exception{
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(12);
        while(System.nanoTime()<deadline){
            if(condition.getAsBoolean())return;
            Thread.sleep(20); // bounded polling; assertions use durable state, not elapsed-time guesses
        }
        fail("timed out: "+description);
    }

    @Test void restartReplaysCommittedPutOverwriteDeleteAndResumesQuorumWrites() throws Exception {
        try(var n2=follower("n2");var n3=follower("n3")) {
            n2.start();online("n2",n2);n3.start();online("n3",n3);
            try(var initial=leader()) {
                initial.start();var c=channel(initial);
                try {
                    assertEquals(ResultCode.RESULT_CODE_OK,put(c,"one","x",new byte[]{1,0}).getCode());
                    assertEquals(ResultCode.RESULT_CODE_OK,put(c,"two","x",new byte[]{9,0,6}).getCode());
                    assertEquals(ResultCode.RESULT_CODE_OK,put(c,"three","z",new byte[0]).getCode());
                    assertEquals(ResultCode.RESULT_CODE_OK,delete(c,"four","z").getCode());
                    eventually(()->n2.statusSnapshot().appliedIndex()==4,"follower's durable COMMIT and apply");
                } finally {c.shutdownNow();}
            }
            live.remove("n3"); // one witness suffices
            try(var restarted=leader()) {
                restarted.start();
                eventually(()->restarted.statusSnapshot().state()==NodeLifecycleState.READY,"recovered READY");
                assertEquals(4,restarted.statusSnapshot().durableCommitIndex());
                assertEquals(4,restarted.statusSnapshot().appliedIndex());
                var c=channel(restarted);
                try {
                    assertArrayEquals(new byte[]{9,0,6},get(c,"x").getValue().toByteArray());
                    assertEquals(4,get(c,"x").getCommitIndex());
                    assertEquals(ResultCode.RESULT_CODE_NOT_FOUND,get(c,"z").getCode());
                    assertEquals(ResultCode.RESULT_CODE_OK,put(c,"five","next",new byte[]{5}).getCode());
                    assertEquals(5,get(c,"next").getCommitIndex());
                    var f=channel(n2);
                    try {assertEquals(ResultCode.RESULT_CODE_NOT_LEADER,get(f,"x").getCode());}
                    finally {f.shutdownNow();}
                } finally {c.shutdownNow();}
            }
        }
        try(var wal=FileWalStore.open(cfg("n1").walFile())){
            assertEquals(5,wal.durableCommitIndex());assertEquals(5,wal.lastDurableLogIndex());
        }
    }

    @Test void unavailableFollowersKeepRecoveredLeaderNotReadyUntilOneReturns() throws Exception {
        try(var n2=follower("n2");var first=leader()){
            n2.start();online("n2",n2);first.start();var c=channel(first);
            try{assertEquals(ResultCode.RESULT_CODE_OK,put(c,"first","x",new byte[]{7}).getCode());
                eventually(()->n2.statusSnapshot().appliedIndex()==1,"original witness commit");
            }finally{c.shutdownNow();}
        }
        live.clear();
        try(var restarted=leader()){
            restarted.start();
            assertEquals(NodeLifecycleState.CONTROL_ONLY,restarted.statusSnapshot().state());
            assertEquals(1,restarted.statusSnapshot().appliedIndex()); // replayed but not admitted
            var c=channel(restarted);
            try {
                assertEquals(ResultCode.RESULT_CODE_NOT_READY,get(c,"x").getCode());
                assertEquals(ResultCode.RESULT_CODE_NOT_READY,put(c,"blocked","x",new byte[]{4}).getCode());
                try(var n2=follower("n2")) {
                    n2.start();online("n2",n2);
                    eventually(()->restarted.statusSnapshot().ready(),"witness returns without new writes");
                    assertArrayEquals(new byte[]{7},get(c,"x").getValue().toByteArray());
                    assertEquals(ResultCode.RESULT_CODE_OK,put(c,"new-id","y",new byte[]{2}).getCode());
                }
            } finally {c.shutdownNow();}
        }
    }

    @Test void leaderUncommittedTailIsRemovedBeforeRecoveryWitnessAndNextIndexReuse() throws Exception {
        try(var n2=follower("n2");var n1=leader()) {
            n2.start();online("n2",n2);n1.start();var c=channel(n1);
            try {
                assertEquals(ResultCode.RESULT_CODE_OK,put(c,"committed","x",new byte[]{1}).getCode());
                eventually(()->n2.statusSnapshot().appliedIndex()==1,"follower committed first entry");
                n2.close();live.remove("n2");
                assertEquals(ResultCode.RESULT_CODE_NO_QUORUM,put(c,"abandoned","x",new byte[]{9}).getCode());
                assertEquals(2,n1.statusSnapshot().lastLogIndex());
                assertEquals(1,n1.statusSnapshot().durableCommitIndex());
            }finally{c.shutdownNow();}
        }
        try(var recovered=leader()) {
            recovered.start();
            assertEquals(NodeLifecycleState.CONTROL_ONLY,recovered.statusSnapshot().state());
            assertEquals(1,recovered.statusSnapshot().lastLogIndex());
            assertEquals(1,recovered.statusSnapshot().appliedIndex());
            try(var n2=follower("n2")) {
                n2.start();online("n2",n2);
                eventually(()->recovered.statusSnapshot().ready(),"tail-free recovered witness");
                var c=channel(recovered);
                try {
                    assertArrayEquals(new byte[]{1},get(c,"x").getValue().toByteArray());
                    assertEquals(ResultCode.RESULT_CODE_OK,put(c,"replacement","x",new byte[]{3}).getCode());
                    assertEquals(2,recovered.statusSnapshot().lastLogIndex());
                }finally{c.shutdownNow();}
            }
        }
        try(var wal=FileWalStore.open(cfg("n1").walFile())) {
            assertEquals(2,wal.durableCommitIndex());
            assertEquals("replacement",wal.recoveryResult().entries().get(1).requestId());
        }
    }

    @Test void recoveredFollowerExtraTailMustBeTruncatedBeforeLeaderReady()throws Exception {
        try(var n2=follower("n2");var n1=leader()) {
            n2.start();online("n2",n2);n1.start();var c=channel(n1);
            try {
                assertEquals(ResultCode.RESULT_CODE_OK,put(c,"old","x",new byte[]{1}).getCode());
                eventually(()->n2.statusSnapshot().appliedIndex()==1,"follower original commit");
            }finally{c.shutdownNow();}
        }
        try(var store=FileWalStore.open(cfg("n2").walFile())){
            var op=Command.put("spare",new byte[]{4});
            store.appendEntryDurably(new LogEntry(2,"spare",op,CanonicalCommandFingerprint.of(op)));
        }
        live.clear();
        try(var n2=follower("n2");var recovered=leader()) {
            n2.start();online("n2",n2);recovered.start();
            eventually(()->recovered.statusSnapshot().ready(),"canonical follower witness after tail truncate");
            assertEquals(1,n2.statusSnapshot().lastLogIndex());
            assertEquals(1,n2.statusSnapshot().durableCommitIndex());
            assertEquals(1,n2.statusSnapshot().appliedIndex());
            var c=channel(recovered);
            try {
                assertEquals(ResultCode.RESULT_CODE_OK,put(c,"new","spare",new byte[]{8}).getCode());
                assertArrayEquals(new byte[]{8},get(c,"spare").getValue().toByteArray());
            }finally{c.shutdownNow();}
        }
    }

    @Test void allThreeProcessesRestartWithTheirOwnExistingWalsAndConverge()throws Exception {
        try(var n2=follower("n2");var n3=follower("n3");var first=leader()) {
            n2.start();online("n2",n2);n3.start();online("n3",n3);first.start();
            var c=channel(first);
            try {
                assertEquals(ResultCode.RESULT_CODE_OK,put(c,"start1","x",new byte[]{1,0}).getCode());
                assertEquals(ResultCode.RESULT_CODE_OK,put(c,"start2","x",new byte[]{2,0}).getCode());
                eventually(()->n2.statusSnapshot().appliedIndex()==2
                        && n3.statusSnapshot().appliedIndex()==2,"initial cluster convergence");
            }finally{c.shutdownNow();}
        }
        live.clear();
        try(var n2=follower("n2");var n3=follower("n3");var restarted=leader()) {
            n2.start();online("n2",n2);n3.start();online("n3",n3);restarted.start();
            eventually(()->restarted.statusSnapshot().ready(),"full cluster recovery certificate");
            assertEquals(2,restarted.statusSnapshot().appliedIndex());
            var c=channel(restarted);
            try {
                assertArrayEquals(new byte[]{2,0},get(c,"x").getValue().toByteArray());
                assertEquals(ResultCode.RESULT_CODE_OK,put(c,"after-all-restart","y",new byte[]{3}).getCode());
                eventually(()->n2.statusSnapshot().appliedIndex()==3
                        && n3.statusSnapshot().appliedIndex()==3,"post-restart full convergence");
                assertArrayEquals(new byte[]{3},n2.followerApplierForTest().diagnosticGetForTest("y").orElseThrow());
                assertArrayEquals(new byte[]{3},n3.followerApplierForTest().diagnosticGetForTest("y").orElseThrow());
            }finally{c.shutdownNow();}
        }
    }

    @Test void recoveredLeaderContinuesIdleCatchUpWhenMissingFollowerReturns()throws Exception {
        try(var n2=follower("n2");var first=leader()) {
            n2.start();online("n2",n2);first.start();var c=channel(first);
            try {
                assertEquals(ResultCode.RESULT_CODE_OK,put(c,"c1","x",new byte[]{4}).getCode());
                assertEquals(ResultCode.RESULT_CODE_OK,put(c,"c2","y",new byte[]{5}).getCode());
                eventually(()->n2.statusSnapshot().appliedIndex()==2,"initial witness applied");
            }finally{c.shutdownNow();}
        }
        live.clear();
        try(var n2=follower("n2");var recovered=leader()) {
            n2.start();online("n2",n2);recovered.start();
            eventually(()->recovered.statusSnapshot().ready(),"recovered leader ready with one witness");
            try(var n3=follower("n3")) {
                n3.start();online("n3",n3);
                eventually(()->n3.statusSnapshot().lastLogIndex()==2
                        && n3.statusSnapshot().durableCommitIndex()==2
                        && n3.statusSnapshot().appliedIndex()==2,"idle catch-up after recovered leader READY");
                assertArrayEquals(new byte[]{4},n3.followerApplierForTest().diagnosticGetForTest("x").orElseThrow());
                assertArrayEquals(new byte[]{5},n3.followerApplierForTest().diagnosticGetForTest("y").orElseThrow());
            }
        }
    }
}
