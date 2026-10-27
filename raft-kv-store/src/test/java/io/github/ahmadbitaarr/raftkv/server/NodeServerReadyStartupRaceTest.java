package io.github.ahmadbitaarr.raftkv.server;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.config.NodeEndpoint;
import io.github.ahmadbitaarr.raftkv.leader.LeaderCoordinator;
import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.replication.GrpcFollowerClientTestSupport;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.wal.FileWalStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/** Deterministic tests at the bound-listener / write-admission boundary. */
class NodeServerReadyStartupRaceTest {
    @TempDir Path root;

    private ClusterConfig config(String id) {
        return ClusterConfigTest.config(root.resolve(id),id);
    }
    private NodeServer leader(Map<String,NodeEndpoint> live,NodeServer.StartupBoundary boundary) {
        return new NodeServer(config("n1"),0,
                cfg -> GrpcFollowerClientTestSupport.withEndpointResolver(cfg,id -> {
                    var endpoint=live.get(id);
                    if(endpoint==null)throw new IllegalArgumentException("follower not started: "+id);
                    return endpoint;
                }),boundary);
    }
    private static ManagedChannel channel(int port) {
        return ManagedChannelBuilder.forAddress("127.0.0.1",port).usePlaintext().build();
    }
    private static KeyValueServiceGrpc.KeyValueServiceBlockingStub client(ManagedChannel channel) {
        return KeyValueServiceGrpc.newBlockingStub(channel).withDeadlineAfter(8,TimeUnit.SECONDS);
    }
    private static MutationResponse put(ManagedChannel channel,String requestId,String key,int value) {
        return client(channel).put(PutRequest.newBuilder().setRequestId(requestId)
                .setKey(key).setValue(ByteString.copyFrom(new byte[]{(byte)value})).build());
    }
    private static GetResponse get(ManagedChannel channel,String key) {
        return client(channel).get(GetRequest.newBuilder().setKey(key).build());
    }
    private static void awaitRelease(CountDownLatch release) throws IOException {
        try {
            if(!release.await(10,TimeUnit.SECONDS))throw new IOException("startup test barrier timed out");
        } catch(InterruptedException interrupted) {
            Thread.currentThread().interrupt();throw new IOException("startup test interrupted",interrupted);
        }
    }
    private static void seed(Path path,boolean committed) throws IOException {
        try(var wal=FileWalStore.open(path)) {
            wal.appendEntryDurably(new LogEntry(1,"seed",Command.put("key",new byte[]{1}),io.github.ahmadbitaarr.raftkv.leader.CanonicalCommandFingerprint.of(Command.put("key",new byte[]{1}))));
            if(committed)wal.appendCommitDurably(1);
        }
    }

    @Test void emptyWalLeaderPublishesReadyBeforeAcceptingFirstMutation() throws Exception {
        CountDownLatch atBoundary=new CountDownLatch(1), release=new CountDownLatch(1);
        CompletableFuture<Integer> port=new CompletableFuture<>();
        ExecutorService worker=Executors.newSingleThreadExecutor();
        try(var node=leader(Map.of(),(boundPort,fault)->{
            port.complete(boundPort);atBoundary.countDown();awaitRelease(release);
        })) {
            Future<?> started=worker.submit(()->{node.start();return null;});
            try {
                assertTrue(atBoundary.await(5,TimeUnit.SECONDS));
                ManagedChannel channel=channel(port.get(5,TimeUnit.SECONDS));
                try {
                    assertEquals(NodeLifecycleState.CONTROL_ONLY,node.statusSnapshot().state());
                    assertFalse(node.statusSnapshot().ready());
                    assertEquals(ResultCode.RESULT_CODE_NOT_READY,put(channel,"first","key",10).getCode(),
                            "the bound listener must not admit mutations before READY publication");
                    assertEquals(ResultCode.RESULT_CODE_NOT_READY,get(channel,"key").getCode());
                    assertEquals(0,node.statusSnapshot().lastLogIndex());
                    release.countDown();
                    started.get(10,TimeUnit.SECONDS);
                    assertEquals(NodeLifecycleState.READY,node.statusSnapshot().state());
                    assertTrue(node.statusSnapshot().ready());
                    assertEquals(ResultCode.RESULT_CODE_NOT_FOUND,get(channel,"key").getCode());
                    assertEquals(0,get(channel,"key").getCommitIndex());
                } finally {channel.shutdownNow();}
            } finally {release.countDown();started.get(10,TimeUnit.SECONDS);}
        } finally {worker.shutdownNow();assertTrue(worker.awaitTermination(5,TimeUnit.SECONDS));}
    }

    @Test void firstWriteRacingWithStartupCommitsAndRemainsReadable() throws Exception {
        CountDownLatch atBoundary=new CountDownLatch(1), release=new CountDownLatch(1);
        CompletableFuture<Integer> port=new CompletableFuture<>();
        ExecutorService worker=Executors.newSingleThreadExecutor();
        var live=new ConcurrentHashMap<String,NodeEndpoint>();
        try(var follower=new NodeServer(config("n2"),0);
            var leader=leader(live,(boundPort,fault)->{
                port.complete(boundPort);atBoundary.countDown();awaitRelease(release);
            })) {
            follower.start();live.put("n2",new NodeEndpoint("127.0.0.1",follower.boundPort()));
            Future<?> started=worker.submit(()->{leader.start();return null;});
            try {
                assertTrue(atBoundary.await(5,TimeUnit.SECONDS));
                ManagedChannel channel=channel(port.get(5,TimeUnit.SECONDS));
                try {
                    assertEquals(ResultCode.RESULT_CODE_NOT_READY,put(channel,"first","x",42).getCode());
                    release.countDown();
                    started.get(10,TimeUnit.SECONDS);
                    var result=put(channel,"first","x",42);
                    assertEquals(ResultCode.RESULT_CODE_OK,result.getCode());
                    assertEquals(1,result.getCommitIndex());
                    assertEquals(NodeLifecycleState.READY,leader.statusSnapshot().state());
                    assertTrue(leader.statusSnapshot().ready());
                    var read=get(channel,"x");
                    assertEquals(ResultCode.RESULT_CODE_OK,read.getCode());
                    assertEquals(1,read.getCommitIndex());
                    assertArrayEquals(new byte[]{42},read.getValue().toByteArray());
                    assertEquals(1,leader.statusSnapshot().appliedIndex());
                } finally {channel.shutdownNow();}
            } finally {release.countDown();started.get(10,TimeUnit.SECONDS);}
        } finally {worker.shutdownNow();assertTrue(worker.awaitTermination(5,TimeUnit.SECONDS));}
    }

    @Test void recoveredNonemptyLeaderNeverOpensWriteOrReadAdmission() throws Exception {
        for(boolean committed:new boolean[]{false,true}) {
            Path walDir=root.resolve(committed?"committed":"uncommitted");
            seed(walDir.resolve("raftkv.wal"),committed);
            ClusterConfig cfg=ClusterConfigTest.config(walDir,"n1");
            try(var leader=new NodeServer(cfg,0)) {
                leader.start();
                assertEquals(NodeLifecycleState.CONTROL_ONLY,leader.statusSnapshot().state());
                assertFalse(leader.statusSnapshot().ready());
                assertEquals(committed?1:0,leader.statusSnapshot().lastLogIndex());
                assertEquals(committed?1:0,leader.statusSnapshot().durableCommitIndex());
                assertEquals(committed?1:0,leader.statusSnapshot().appliedIndex());
                assertFalse(leader.statusSnapshot().ready(),
                        "local WAL recovery alone must not authorize client data readiness");
                ManagedChannel channel=channel(leader.boundPort());
                try {
                    assertEquals(ResultCode.RESULT_CODE_NOT_READY,get(channel,"key").getCode());
                    assertEquals(ResultCode.RESULT_CODE_NOT_READY,put(channel,"new","key",99).getCode());
                } finally {channel.shutdownNow();}
            }
        }
    }

    @Test void fatalStorageFaultDuringStartupBoundaryCannotPublishReadyOrReopenAdmission() throws Exception {
        IOException injected=new IOException("injected fatal storage fault at admission boundary");
        var leader=leader(Map.of(),(port,reportFault)->reportFault.accept(injected));
        IOException startup=assertThrows(IOException.class,leader::start);
        assertSame(injected,startup.getCause());
        assertEquals(NodeLifecycleState.FAILED,leader.statusSnapshot().state());
        assertFalse(leader.statusSnapshot().ready());
        assertEquals(0,leader.statusSnapshot().lastLogIndex());
        assertEquals(0,leader.statusSnapshot().durableCommitIndex());
        assertEquals(0,leader.statusSnapshot().appliedIndex());
        assertEquals(LeaderCoordinator.Code.NOT_READY,
                leader.leaderCoordinatorForTest().mutate("after-fault",Command.put("x",new byte[]{1})).code());
        assertEquals(io.github.ahmadbitaarr.raftkv.leader.LeaderReadCoordinator.Code.NOT_READY,
                leader.readCoordinatorForTest().read("x").code());
        assertThrows(IOException.class,leader::close);
        assertEquals(NodeLifecycleState.STOPPED,leader.statusSnapshot().state());
        try(var lease=WalDirectoryLease.acquire(config("n1").walDirectory())) {
            assertTrue(lease.isHeld());
        }
    }

    @Test void shutdownRacingStartupCannotReopenReadOrWriteAdmission() throws Exception {
        CountDownLatch atBoundary=new CountDownLatch(1), release=new CountDownLatch(1);
        ExecutorService workers=Executors.newFixedThreadPool(2);
        try(var leader=leader(Map.of(),(port,fault)->{
            atBoundary.countDown();awaitRelease(release);
        })) {
            Future<?> started=workers.submit(()->{leader.start();return null;});
            try {
                assertTrue(atBoundary.await(5,TimeUnit.SECONDS));
                CountDownLatch requestedClose=new CountDownLatch(1);
                Future<?> closing=workers.submit(()->{
                    requestedClose.countDown();leader.close();return null;
                });
                assertTrue(requestedClose.await(5,TimeUnit.SECONDS));
                release.countDown();
                started.get(10,TimeUnit.SECONDS);
                closing.get(10,TimeUnit.SECONDS);
                assertEquals(NodeLifecycleState.STOPPED,leader.statusSnapshot().state());
                assertFalse(leader.statusSnapshot().ready());
                assertEquals(io.github.ahmadbitaarr.raftkv.leader.LeaderReadCoordinator.Code.NOT_READY,
                        leader.readCoordinatorForTest().read("x").code());
                assertEquals(LeaderCoordinator.Code.NOT_READY,
                        leader.leaderCoordinatorForTest().mutate("after-close",Command.put("x",new byte[]{1})).code());
                assertThrows(IllegalStateException.class,leader::start);
                assertEquals(0,leader.statusSnapshot().lastLogIndex());
            } finally {release.countDown();}
        } finally {workers.shutdownNow();assertTrue(workers.awaitTermination(5,TimeUnit.SECONDS));}
    }
}
