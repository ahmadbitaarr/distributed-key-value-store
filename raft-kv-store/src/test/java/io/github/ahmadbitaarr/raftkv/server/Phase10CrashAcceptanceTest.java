package io.github.ahmadbitaarr.raftkv.server;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.github.ahmadbitaarr.raftkv.config.*;
import io.github.ahmadbitaarr.raftkv.leader.CanonicalCommandFingerprint;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.recovery.RecoveryManager;
import io.github.ahmadbitaarr.raftkv.replication.GrpcFollowerClientTestSupport;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.statemachine.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

/** Phase 10 acceptance: hard process exits at precise WAL and real-Netty boundaries. */
class Phase10CrashAcceptanceTest {
    @TempDir Path root;
    private final Map<String,NodeEndpoint> online=new ConcurrentHashMap<>();
    private ClusterConfig cfg(String id){return ClusterConfigTest.config(root.resolve(id),id);}
    private NodeServer node(String id){
        return id.equals("n1")?new NodeServer(cfg(id),0,c ->
                GrpcFollowerClientTestSupport.withEndpointResolver(c,peer -> {
                    NodeEndpoint ep=online.get(peer);
                    if(ep==null)throw new IllegalArgumentException("offline: "+peer);
                    return ep;
                })):new NodeServer(cfg(id),0);
    }
    private void available(String id,NodeServer node){
        assertTrue(node.boundPort()>0);
        online.put(id,new NodeEndpoint("127.0.0.1",node.boundPort()));
    }
    private static LogEntry entry(int n){
        var cmd=Command.put("key-"+n,new byte[]{(byte)n,0});
        return new LogEntry(n,"crash-netty-"+n,cmd,CanonicalCommandFingerprint.of(cmd));
    }
    private static void converge(BooleanSupplier done,String label)throws Exception{
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(12);
        while(System.nanoTime()<deadline){
            if(done.getAsBoolean())return;
            Thread.sleep(20); // bounded eventual assertion, not a fixed delay as evidence
        }
        fail("convergence timeout: "+label);
    }
    private static int child(String fixture,String mode,Path fileOrDirectory,int expectedExit)throws Exception {
        String java=Path.of(System.getProperty("java.home"),"bin",
                System.getProperty("os.name").toLowerCase().contains("win")?"java.exe":"java").toString();
        Process child=new ProcessBuilder(java,"-cp",System.getProperty("java.class.path"),fixture,
                mode,fileOrDirectory.toString()).redirectErrorStream(true).start();
        if(!child.waitFor(12,TimeUnit.SECONDS)){
            child.destroyForcibly();fail("child failed to reach boundary: "+mode);
        }
        assertEquals(expectedExit,child.exitValue(),"child did not halt at approved boundary: "+mode);
        return child.exitValue();
    }
    private static GetResponse get(NodeServer n,String key){
        ManagedChannel channel=ManagedChannelBuilder.forAddress("127.0.0.1",n.boundPort())
                .usePlaintext().build();
        try {return KeyValueServiceGrpc.newBlockingStub(channel).withDeadlineAfter(3,TimeUnit.SECONDS)
                .get(GetRequest.newBuilder().setKey(key).build());}
        finally{channel.shutdownNow();}
    }
    private static MutationResponse put(NodeServer n,String id,String key){
        ManagedChannel channel=ManagedChannelBuilder.forAddress("127.0.0.1",n.boundPort())
                .usePlaintext().build();
        try {return KeyValueServiceGrpc.newBlockingStub(channel).withDeadlineAfter(3,TimeUnit.SECONDS)
                .put(PutRequest.newBuilder().setRequestId(id).setKey(key)
                        .setValue(com.google.protobuf.ByteString.copyFrom(new byte[]{9})).build());}
        finally{channel.shutdownNow();}
    }

    @Test void f01HardExitBeforeEntryForceCannotInventCommittedOrAppliedMutation()throws Exception {
        Path wal=root.resolve("n1/raftkv.wal");
        child("io.github.ahmadbitaarr.raftkv.wal.Phase10WalCrashBoundaryProbe",
                "BEFORE_ENTRY_FORCE",wal,41);
        // A full frame may remain in the OS cache despite no force; it has NO COMMIT authority.
        try(var store=FileWalStore.open(wal)){
            assertEquals(0,store.durableCommitIndex());
            var applier=new StateMachineApplier(new KvStateMachine());
            var local=RecoveryManager.recoverLocal(new WalBackedPersistentLog(store),store,applier);
            assertEquals(0,local.appliedIndex());
            assertEquals(0,store.durableCommitIndex());
            assertEquals(0,store.lastDurableLogIndex());
            assertTrue(applier.diagnosticGetForTest("crash-1").isEmpty());
        }
        try(var restarted=node("n1")){
            restarted.start();
            assertFalse(restarted.statusSnapshot().ready(),"preexisting WAL requires a witness");
            assertEquals(NodeLifecycleState.CONTROL_ONLY,restarted.statusSnapshot().state());
            assertEquals(ResultCode.RESULT_CODE_NOT_READY,get(restarted,"crash-1").getCode());
        }
    }

    @Test void f03HardExitAfterRealFollowerDurableAckBeforeLeaderCommitCannotFabricateCommit()throws Exception {
        child("io.github.ahmadbitaarr.raftkv.server.Phase10NettyCrashProbe",
                "FOLLOWER_ACK_BEFORE_LEADER_COMMIT",root,44);
        try(var leader=FileWalStore.open(cfg("n1").walFile());
            var follower=FileWalStore.open(cfg("n2").walFile())){
            assertEquals(1,leader.lastDurableLogIndex());
            assertEquals(0,leader.durableCommitIndex());
            assertEquals(1,follower.lastDurableLogIndex(),"actual gRPC durable ACK survived halt");
            assertEquals(0,follower.durableCommitIndex());
            var apply=new StateMachineApplier(new KvStateMachine());
            RecoveryManager.recoverLocal(new WalBackedPersistentLog(leader),leader,apply);
            assertEquals(0,apply.appliedIndex());
            assertEquals(0,leader.lastDurableLogIndex());
            assertTrue(apply.diagnosticGetForTest("key-1").isEmpty());
        }
        try(var restarted=node("n1")){
            restarted.start();
            assertEquals(NodeLifecycleState.CONTROL_ONLY,restarted.statusSnapshot().state());
            assertEquals(ResultCode.RESULT_CODE_NOT_READY,get(restarted,"key-1").getCode());
        }
    }

    @Test void f06HardExitBeforeAtomicRewriteReplacementRetainsCorrectAuthoritativePrefix()throws Exception {
        verifyInterruptedRewrite("REWRITE_BEFORE_MOVE",42);
    }
    @Test void f06HardExitAfterAtomicRewriteReplacementRetainsCorrectAuthoritativePrefix()throws Exception {
        verifyInterruptedRewrite("REWRITE_AFTER_MOVE",43);
    }
    private void verifyInterruptedRewrite(String mode,int exit)throws Exception {
        Path wal=root.resolve("n1/raftkv.wal");
        child("io.github.ahmadbitaarr.raftkv.wal.Phase10WalCrashBoundaryProbe",mode,wal,exit);
        try(var store=FileWalStore.open(wal)){
            assertEquals(1,store.durableCommitIndex());
            assertEquals(mode.equals("REWRITE_BEFORE_MOVE")?2:1,store.lastDurableLogIndex());
            var applier=new StateMachineApplier(new KvStateMachine());
            RecoveryManager.recoverLocal(new WalBackedPersistentLog(store),store,applier);
            assertEquals(1,store.lastDurableLogIndex());
            assertEquals(1,store.durableCommitIndex());
            assertEquals(1,applier.appliedIndex());
            assertArrayEquals(new byte[]{1,0},applier.diagnosticGetForTest("crash-1").orElseThrow());
            assertTrue(applier.diagnosticGetForTest("crash-2").isEmpty());
        }
        try(var restarted=node("n1")){
            restarted.start();
            assertEquals(NodeLifecycleState.CONTROL_ONLY,restarted.statusSnapshot().state());
            assertFalse(restarted.statusSnapshot().ready());
            assertEquals(1,restarted.statusSnapshot().durableCommitIndex());
            assertEquals(1,restarted.statusSnapshot().appliedIndex());
            assertEquals(ResultCode.RESULT_CODE_NOT_READY,get(restarted,"crash-1").getCode());
        }
    }

    @Test void f07HardExitDuringRealNettyReconciliationCannotCreateEarlyWitnessAndSafelyResumes()throws Exception {
        child("io.github.ahmadbitaarr.raftkv.server.Phase10NettyCrashProbe",
                "RECONCILIATION_AFTER_FIRST_ACK",root,45);
        try(var leaderWal=FileWalStore.open(cfg("n1").walFile());
            var followerWal=FileWalStore.open(cfg("n2").walFile())){
            assertEquals(2,leaderWal.durableCommitIndex());
            assertEquals(2,leaderWal.lastDurableLogIndex());
            assertEquals(1,followerWal.lastDurableLogIndex());
            assertEquals(0,followerWal.durableCommitIndex());
        }
        try(var restarted=node("n1")){
            restarted.start();
            assertEquals(NodeLifecycleState.CONTROL_ONLY,restarted.statusSnapshot().state());
            assertEquals(ResultCode.RESULT_CODE_NOT_READY,get(restarted,"key-1").getCode());
            try(var follower=node("n2")){
                follower.start();available("n2",follower);
                converge(()->restarted.statusSnapshot().ready(),"recovery witness after interrupted reconciliation");
                converge(()->follower.statusSnapshot().appliedIndex()==2,"follower COMMIT and apply after replay");
                assertEquals(2,follower.statusSnapshot().durableCommitIndex());
                assertEquals(2,follower.statusSnapshot().lastLogIndex());
                assertArrayEquals(new byte[]{1,0},get(restarted,"key-1").getValue().toByteArray());
                assertArrayEquals(new byte[]{2,0},get(restarted,"key-2").getValue().toByteArray());
            }
        }
    }

    @Test void recoveredReadyLeaderFailsClosedWhenBackgroundNettyFindsCommittedHistoryAhead()throws Exception {
        try(var leader=FileWalStore.open(cfg("n1").walFile());
            var safe=FileWalStore.open(cfg("n2").walFile());
            var contradictory=FileWalStore.open(cfg("n3").walFile())){
            for(var store:new FileWalStore[]{leader,safe,contradictory}){
                store.appendEntryDurably(entry(1));store.appendCommitDurably(1);
            }
            contradictory.appendEntryDurably(entry(2));
            contradictory.appendCommitDurably(2); // genuine committed-ahead contradiction
        }
        try(var safe=node("n2");var contradictory=node("n3")){
            safe.start();available("n2",safe);
            NodeServer leader=node("n1");
            try {
                leader.start();
                converge(()->leader.statusSnapshot().ready(),"one valid follower recovery witness");
                assertEquals(ResultCode.RESULT_CODE_OK,get(leader,"key-1").getCode());
                contradictory.start();available("n3",contradictory);
                converge(()->leader.statusSnapshot().state()==NodeLifecycleState.FAILED,
                        "background committed-history contradiction must reach NodeServer");
                assertFalse(leader.statusSnapshot().ready());
                assertEquals(ResultCode.RESULT_CODE_NOT_READY,get(leader,"key-1").getCode());
                assertEquals(ResultCode.RESULT_CODE_NOT_READY,put(leader,"no-after-fault","key-new").getCode());
                assertEquals(1,leader.statusSnapshot().lastLogIndex());
                assertEquals(1,leader.statusSnapshot().durableCommitIndex());
                assertEquals(1,leader.statusSnapshot().appliedIndex());
                assertEquals(2,contradictory.statusSnapshot().durableCommitIndex(),
                        "unsafe follower history was never truncated");
            } finally {
                try {leader.close();}
                catch(IOException expected){assertTrue(expected.toString().contains("fatal"));}
            }
        }
    }
}
