package io.github.ahmadbitaarr.raftkv.server;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.github.ahmadbitaarr.raftkv.config.*;
import io.github.ahmadbitaarr.raftkv.replication.GrpcFollowerClientTestSupport;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

/** Real Netty/gRPC and durable WAL assertions, never result-code-only dedup tests. */
class Phase11RealThreeNodeDedupTest {
    @TempDir Path root;
    private final Map<String,NodeEndpoint> live=new ConcurrentHashMap<>();
    private ClusterConfig cfg(String id){
        var members=ClusterConfigTest.members();
        return new ClusterConfig("phase11",id,"n1",members,"127.0.0.1",members.get(id).port(),
                root.resolve(id),4000,1500,600,2000,12582912);
    }
    private NodeServer follower(String id){return new NodeServer(cfg(id),0);}
    private NodeServer leader(){return new NodeServer(cfg("n1"),0,c ->
            GrpcFollowerClientTestSupport.withEndpointResolver(c,id -> {
                var ep=live.get(id);if(ep==null)throw new IllegalArgumentException("offline: "+id);return ep;
            }));}
    private void online(String id,NodeServer node){assertTrue(node.boundPort()>0);live.put(id,new NodeEndpoint("127.0.0.1",node.boundPort()));}
    private static ManagedChannel channel(NodeServer n){return ManagedChannelBuilder.forAddress("127.0.0.1",n.boundPort()).usePlaintext().build();}
    private static KeyValueServiceGrpc.KeyValueServiceBlockingStub stub(ManagedChannel c){return KeyValueServiceGrpc.newBlockingStub(c).withDeadlineAfter(8,TimeUnit.SECONDS);}
    private static MutationResponse put(ManagedChannel c,String id,String key,byte[] data){return stub(c).put(PutRequest.newBuilder().setRequestId(id).setKey(key).setValue(ByteString.copyFrom(data)).build());}
    private static MutationResponse del(ManagedChannel c,String id,String key){return stub(c).delete(DeleteRequest.newBuilder().setRequestId(id).setKey(key).build());}
    private static GetResponse get(ManagedChannel c,String key){return stub(c).get(GetRequest.newBuilder().setKey(key).build());}
    private static void eventually(BooleanSupplier test,String label)throws Exception{
        long limit=System.nanoTime()+TimeUnit.SECONDS.toNanos(12);
        while(System.nanoTime()<limit){if(test.getAsBoolean())return;Thread.sleep(20);}
        fail("timed out: "+label);
    }
    private static void reply(MutationResponse r,long index){assertEquals(ResultCode.RESULT_CODE_OK,r.getCode());assertTrue(r.hasCommitIndex());assertEquals(index,r.getCommitIndex());}

    @Test void exactPutAndDeleteRetriesDoNotAppendReapplyOrAdvanceCommit()throws Exception {
        try(var n2=follower("n2");var n3=follower("n3");var n1=leader()) {
            n2.start();online("n2",n2);n3.start();online("n3",n3);n1.start();
            var c=channel(n1);
            try {
                reply(put(c,"p","binary",new byte[]{0,-1,0}),1);
                reply(put(c,"p","binary",new byte[]{0,-1,0}),1);
                assertEquals(ResultCode.RESULT_CODE_REQUEST_ID_CONFLICT,put(c,"p","binary",new byte[]{0,-2,0}).getCode());
                assertEquals(ResultCode.RESULT_CODE_REQUEST_ID_CONFLICT,del(c,"p","binary").getCode());
                reply(del(c,"d","binary"),2);reply(del(c,"d","binary"),2);
                assertEquals(ResultCode.RESULT_CODE_REQUEST_ID_CONFLICT,put(c,"d","binary",new byte[0]).getCode());
                assertEquals(ResultCode.RESULT_CODE_NOT_FOUND,get(c,"binary").getCode());
                assertEquals(2,n1.statusSnapshot().lastLogIndex());
                assertEquals(2,n1.statusSnapshot().durableCommitIndex());
                assertEquals(2,n1.statusSnapshot().appliedIndex());
            } finally {c.shutdownNow();}
        }
        try(var persisted=FileWalStore.open(cfg("n1").walFile())){
            assertEquals(2,persisted.lastDurableLogIndex());assertEquals(2,persisted.durableCommitIndex());
            assertEquals("p",persisted.recoveryResult().entries().get(0).requestId());
            assertEquals("d",persisted.recoveryResult().entries().get(1).requestId());
        }
    }
    private static final class BlockingEntryWal implements WalStore {
        private final FileWalStore delegate;
        private final CountDownLatch entered,release;
        private BlockingEntryWal(Path path,CountDownLatch entered,CountDownLatch release)throws IOException {
            delegate=FileWalStore.open(path);this.entered=entered;this.release=release;
        }
        @Override public WalRecoveryResult recoveryResult(){return delegate.recoveryResult();}
        @Override public long lastDurableLogIndex(){return delegate.lastDurableLogIndex();}
        @Override public long durableCommitIndex(){return delegate.durableCommitIndex();}
        @Override public void appendEntryDurably(LogEntry entry)throws IOException {
            entered.countDown();
            try {if(!release.await(8,TimeUnit.SECONDS))throw new IOException("ENTRY-force barrier timed out");}
            catch(InterruptedException x){Thread.currentThread().interrupt();throw new IOException("ENTRY-force barrier interrupted",x);}
            delegate.appendEntryDurably(entry);
        }
        @Override public void appendCommitDurably(long index)throws IOException{delegate.appendCommitDurably(index);}
        @Override public void rewriteDurably(List<LogEntry> retained,long index)throws IOException{delegate.rewriteDurably(retained,index);}
        @Override public void close()throws IOException{delegate.close();}
    }
    @Test void concurrentIdenticalNettyPutsShareOneLogicalMutation()throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var n2=follower("n2");var n3=follower("n3");
            var n1=new NodeServer(cfg("n1"),0,path->new BlockingEntryWal(path,entered,release),c ->
                GrpcFollowerClientTestSupport.withEndpointResolver(c,id -> {
                    var ep=live.get(id);if(ep==null)throw new IllegalArgumentException("offline: "+id);return ep;
                }))) {
            n2.start();online("n2",n2);n3.start();online("n3",n3);n1.start();
            var a=channel(n1);var b=channel(n1);var conflict=channel(n1);var pool=Executors.newFixedThreadPool(2);
            try {
                var first=pool.submit(()->put(a,"concurrent","x",new byte[]{1}));
                assertTrue(entered.await(3,TimeUnit.SECONDS),"owner reached real WAL ENTRY force");
                assertEquals(ResultCode.RESULT_CODE_REQUEST_ID_CONFLICT,
                        put(conflict,"concurrent","x",new byte[]{2}).getCode(),
                        "conflicting real Netty RPC must reject during blocked owner WAL force");
                var secondStarted=new CountDownLatch(1);
                var second=pool.submit(()->{secondStarted.countDown();return put(b,"concurrent","x",new byte[]{1});});
                assertTrue(secondStarted.await(2,TimeUnit.SECONDS));
                release.countDown();
                reply(first.get(8,TimeUnit.SECONDS),1);reply(second.get(8,TimeUnit.SECONDS),1);
                assertEquals(1,n1.statusSnapshot().lastLogIndex());assertEquals(1,n1.statusSnapshot().appliedIndex());
            }finally{release.countDown();pool.shutdownNow();a.shutdownNow();b.shutdownNow();conflict.shutdownNow();}
        }
        try(var wal=FileWalStore.open(cfg("n1").walFile())){
            assertEquals(1,wal.lastDurableLogIndex());assertEquals(1,wal.durableCommitIndex());
        }
    }
    @Test void committedHistoryIsReconstructedAndOriginalIndexesRetainedAfterRestart()throws Exception {
        try(var n2=follower("n2");var n3=follower("n3")){
            n2.start();online("n2",n2);n3.start();online("n3",n3);
            try(var first=leader()){
                first.start();var c=channel(first);
                try {reply(put(c,"before-crash","x",new byte[]{4,0}),1);
                    reply(del(c,"before-delete","z"),2);
                    eventually(()->n2.statusSnapshot().durableCommitIndex()==2 && n2.statusSnapshot().appliedIndex()==2,"follower witness");
                }finally{c.shutdownNow();}
            }
            live.remove("n3");
            try(var recovered=leader()){
                recovered.start();eventually(()->recovered.statusSnapshot().ready(),"recovered witness authorization");
                var c=channel(recovered);
                try {
                    reply(put(c,"before-crash","x",new byte[]{4,0}),1);
                    reply(del(c,"before-delete","z"),2);
                    assertEquals(ResultCode.RESULT_CODE_REQUEST_ID_CONFLICT,put(c,"before-crash","x",new byte[]{5}).getCode());
                    assertEquals(2,recovered.statusSnapshot().lastLogIndex());
                    reply(put(c,"new-after-recovery","y",new byte[]{9}),3);
                }finally{c.shutdownNow();}
            }
        }
        try(var wal=FileWalStore.open(cfg("n1").walFile())){
            assertEquals(3,wal.durableCommitIndex());assertEquals("new-after-recovery",wal.recoveryResult().entries().get(2).requestId());
        }
    }
    @Test void leaderWithoutRecoveryWitnessDoesNotExposeCommittedDuplicates()throws Exception {
        try(var n2=follower("n2");var first=leader()){
            n2.start();online("n2",n2);first.start();var c=channel(first);
            try{reply(put(c,"original","x",new byte[]{8}),1);
                eventually(()->n2.statusSnapshot().appliedIndex()==1,"first witness commit");
            }finally{c.shutdownNow();}
        }
        live.clear();
        try(var recovered=leader()){
            recovered.start();assertEquals(NodeLifecycleState.CONTROL_ONLY,recovered.statusSnapshot().state());
            var c=channel(recovered);
            try {
                assertEquals(ResultCode.RESULT_CODE_NOT_READY,put(c,"original","x",new byte[]{8}).getCode());
                assertEquals(ResultCode.RESULT_CODE_NOT_READY,get(c,"x").getCode());
                assertEquals(1,recovered.statusSnapshot().durableCommitIndex());
                assertEquals(1,recovered.statusSnapshot().appliedIndex());
            }finally{c.shutdownNow();}
        }
    }
    @Test void duplicateCommittedResultSurvivesLossOfSecondFollowerQuorum()throws Exception {
        var n2=follower("n2");var n3=follower("n3");
        try(var n1=leader()){
            n2.start();online("n2",n2);n3.start();online("n3",n3);n1.start();
            var c=channel(n1);
            try {
                reply(put(c,"already-committed","x",new byte[]{1}),1);
                n2.close();live.remove("n2");n3.close();live.remove("n3");
                reply(put(c,"already-committed","x",new byte[]{1}),1); // no new quorum
                assertEquals(1,n1.statusSnapshot().lastLogIndex());
                assertEquals(1,n1.statusSnapshot().durableCommitIndex());
            }finally{c.shutdownNow();}
        }finally{if(n2.statusSnapshot().state()!=NodeLifecycleState.STOPPED)n2.close();
            if(n3.statusSnapshot().state()!=NodeLifecycleState.STOPPED)n3.close();}
    }
    @Test void catchUpAndUnrelatedWritesCannotChangeOriginalDedupIndex()throws Exception {
        NodeServer n3=null;
        try(var n2=follower("n2");var n1=leader()){
            n2.start();online("n2",n2);n3=follower("n3");n3.start();online("n3",n3);n1.start();
            try {
                var c=channel(n1);
                try {
                    reply(put(c,"first","x",new byte[0]),1);
                    NodeServer old=n3;eventually(()->old.statusSnapshot().appliedIndex()==1,"initial replica");
                    n3.close();live.remove("n3");n3=null;
                    reply(put(c,"second","y",new byte[]{2}),2);
                    n3=follower("n3");n3.start();online("n3",n3);
                    NodeServer returned=n3;eventually(()->returned.statusSnapshot().appliedIndex()==2,"idle follower catch-up");
                    reply(put(c,"third","z",new byte[]{3}),3);
                    reply(put(c,"first","x",new byte[0]),1);
                    assertEquals(ResultCode.RESULT_CODE_REQUEST_ID_CONFLICT,put(c,"first","x",new byte[]{1}).getCode());
                    assertEquals(3,n1.statusSnapshot().durableCommitIndex());
                }finally{c.shutdownNow();}
            }finally{if(n3!=null)n3.close();}
        }
    }

    @Test void hardExitAfterLeaderCommitForceBeforeClientReplyRecoversExactIdWithoutSecondEntry()throws Exception {
        String java=Path.of(System.getProperty("java.home"),"bin",
                System.getProperty("os.name").toLowerCase().contains("win")?"java.exe":"java").toString();
        Process child=new ProcessBuilder(java,"-cp",System.getProperty("java.class.path"),
                Phase11CommitBeforeResponseCrashProbe.class.getName(),root.toString())
                .redirectErrorStream(true).start();
        if(!child.waitFor(16,TimeUnit.SECONDS)){
            child.destroyForcibly();fail("child never reached forced COMMIT boundary");
        }
        assertEquals(57,child.exitValue(),"child must halt after real leader COMMIT force");
        try(var wal=FileWalStore.open(cfg("n1").walFile())){
            assertEquals(1,wal.lastDurableLogIndex());assertEquals(1,wal.durableCommitIndex());
            assertEquals("hard-crash-fixed-id",wal.recoveryResult().entries().get(0).requestId());
        }
        try(var n2=follower("n2");var n1=leader()){
            n2.start();online("n2",n2);
            n1.start();eventually(()->n1.statusSnapshot().ready(),"durable follower witness after hard crash");
            var channel=channel(n1);
            try{
                reply(put(channel,"hard-crash-fixed-id","hard-crash-key",new byte[]{0,9}),1);
                assertEquals(1,n1.statusSnapshot().lastLogIndex());
                assertEquals(1,n1.statusSnapshot().durableCommitIndex());
                assertEquals(1,n1.statusSnapshot().appliedIndex());
                assertArrayEquals(new byte[]{0,9},get(channel,"hard-crash-key").getValue().toByteArray());
                reply(put(channel,"fresh-new-id","new",new byte[]{1}),2);
            }finally{channel.shutdownNow();}
        }
        try(var wal=FileWalStore.open(cfg("n1").walFile())){
            assertEquals(2,wal.durableCommitIndex());
            assertEquals("hard-crash-fixed-id",wal.recoveryResult().entries().get(0).requestId());
            assertEquals("fresh-new-id",wal.recoveryResult().entries().get(1).requestId());
        }
    }

    @Test void discardedUncommittedIdIsNotDeduplicatedAfterSafeRecoveryTailRewrite()throws Exception {
        var cfg=cfg("n1");
        var committed=io.github.ahmadbitaarr.raftkv.log.Command.put("x",new byte[]{1});
        var discarded=io.github.ahmadbitaarr.raftkv.log.Command.put("y",new byte[]{9});
        try(var wal=FileWalStore.open(cfg.walFile())){
            wal.appendEntryDurably(new io.github.ahmadbitaarr.raftkv.log.LogEntry(1,"original",committed,
                    io.github.ahmadbitaarr.raftkv.leader.CanonicalCommandFingerprint.of(committed)));
            wal.appendCommitDurably(1);
            wal.appendEntryDurably(new io.github.ahmadbitaarr.raftkv.log.LogEntry(2,"discarded-id",discarded,
                    io.github.ahmadbitaarr.raftkv.leader.CanonicalCommandFingerprint.of(discarded)));
        }
        try(var n2=follower("n2");var recovered=leader()){
            n2.start();online("n2",n2);recovered.start();
            eventually(()->recovered.statusSnapshot().ready(),"canonical recovery witness");
            var c=channel(recovered);
            try {
                assertEquals(ResultCode.RESULT_CODE_NOT_FOUND,get(c,"y").getCode());
                reply(put(c,"discarded-id","y",new byte[]{2}),2);
                assertArrayEquals(new byte[]{2},get(c,"y").getValue().toByteArray());
                assertEquals(2,recovered.statusSnapshot().durableCommitIndex());
            }finally{c.shutdownNow();}
        }
        try(var wal=FileWalStore.open(cfg.walFile())){
            assertEquals(2,wal.durableCommitIndex());
            assertEquals("discarded-id",wal.recoveryResult().entries().get(1).requestId());
            assertArrayEquals(new byte[]{2},wal.recoveryResult().entries().get(1).command().value().orElseThrow());
        }
    }

    @Test void fullThreeNodeWalRestartReconstructsDedupAndFollowerState()throws Exception {
        try(var n2=follower("n2");var n3=follower("n3");var first=leader()){
            n2.start();online("n2",n2);n3.start();online("n3",n3);first.start();
            var c=channel(first);
            try {
                reply(put(c,"cluster-put","x",new byte[]{0,2}),1);
                reply(del(c,"cluster-delete","z"),2);
                eventually(()->n2.statusSnapshot().appliedIndex()==2 && n3.statusSnapshot().appliedIndex()==2,
                        "both durable followers applied original commits");
            }finally{c.shutdownNow();}
        }
        live.clear();
        try(var n2=follower("n2");var n3=follower("n3");var recovered=leader()){
            n2.start();online("n2",n2);n3.start();online("n3",n3);recovered.start();
            eventually(()->recovered.statusSnapshot().ready(),"full cluster recovered witness");
            var c=channel(recovered);
            try {
                reply(put(c,"cluster-put","x",new byte[]{0,2}),1);
                reply(del(c,"cluster-delete","z"),2);
                assertEquals(2,recovered.statusSnapshot().lastLogIndex());
                reply(put(c,"fresh-after-all-restart","y",new byte[]{8}),3);
                eventually(()->n2.statusSnapshot().appliedIndex()==3 && n3.statusSnapshot().appliedIndex()==3,
                        "followers converge after new mutation");
                assertArrayEquals(new byte[]{0,2},get(c,"x").getValue().toByteArray());
            }finally{c.shutdownNow();}
        }
        try(var wal=FileWalStore.open(cfg("n1").walFile())){
            assertEquals(3,wal.durableCommitIndex());
            assertEquals("cluster-put",wal.recoveryResult().entries().get(0).requestId());
        }
    }
}
