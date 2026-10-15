package io.github.ahmadbitaarr.raftkv.replication;

import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.github.ahmadbitaarr.raftkv.config.*;
import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.log.WalBackedPersistentLog;
import io.github.ahmadbitaarr.raftkv.rpc.*;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import io.github.ahmadbitaarr.raftkv.server.NodeRole;
import io.github.ahmadbitaarr.raftkv.server.NodeLifecycleState;
import io.github.ahmadbitaarr.raftkv.server.NodeStatusSnapshot;
import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

class GrpcFollowerClientLoopbackTest {
    @TempDir Path dir;
    private static LogEntry entry(long n){return new LogEntry(n,"id"+n,Command.put("bin",new byte[]{0,-1,127}),new byte[]{1,2});}

    private static final class Fixture implements AutoCloseable {
        final FileWalStore store;
        final WalBackedPersistentLog log;
        final ReplicationOperationGate gate=new ReplicationOperationGate();
        final AtomicReference<NodeStatusSnapshot> snapshot;
        final String id;
        final Server server;
        final GrpcFollowerClient client;
        Fixture(Path path)throws Exception { this(path,"n2"); }
        Fixture(Path path,String id)throws Exception {
            this.id=id;
            snapshot=new AtomicReference<>(new NodeStatusSnapshot(id,NodeRole.FOLLOWER,NodeLifecycleState.CONTROL_ONLY,0,0,0,false));
            store=FileWalStore.open(path);
            log=new WalBackedPersistentLog(store);
            var cfg=ClusterConfigTest.config(path.getParent(),id);
            FollowerReplicationHandler handler=new FollowerReplicationHandler(cfg,log,store,gate,snapshot::get,
                n->snapshot.updateAndGet(s->new NodeStatusSnapshot(id,NodeRole.FOLLOWER,s.state(),n,s.durableCommitIndex(),0,false)),
                io->snapshot.updateAndGet(s->s.withState(NodeLifecycleState.FAILED)));
            server=NettyServerBuilder.forPort(0).maxInboundMessageSize(cfg.maxInboundMessageBytes())
                    .addService(new ReplicationGrpcService(cfg,snapshot::get,handler)).build().start();
            gate.open();
            client=new GrpcFollowerClient(ClusterConfigTest.config(path.getParent().resolve("leader"),"n1"),
                    followerId->new NodeEndpoint("127.0.0.1",server.getPort()));
        }
        public void close()throws Exception {
            gate.closeAdmission();
            server.shutdownNow();
            assertTrue(server.awaitTermination(5,TimeUnit.SECONDS));
            assertTrue(gate.awaitDrained(5,TimeUnit.SECONDS));
            client.close();log.close();
        }
    }

    @Test void realGrpcAckOccursAfterFollowerDurableAppend()throws Exception {
        Path wal=dir.resolve("follower.wal");
        try(Fixture f=new Fixture(wal)){
            var outcome=f.client.replicate("n2",entry(1));
            assertEquals(FollowerClient.Kind.ACK,outcome.kind());
            assertEquals(1,f.store.lastDurableLogIndex());
            assertEquals(entry(1),f.log.get(1).orElseThrow());
            assertFalse(f.snapshot.get().ready());
        }
        try(var reopened=WalBackedPersistentLog.open(wal)){
            assertEquals(entry(1),reopened.get(1).orElseThrow());
        }
    }
    @Test void realGrpcDuplicateDoesNotWriteSecondFrame()throws Exception {
        try(Fixture f=new Fixture(dir.resolve("wal"))){
            assertEquals(FollowerClient.Kind.ACK,f.client.replicate("n2",entry(1)).kind());
            long size=java.nio.file.Files.size(dir.resolve("wal"));
            assertEquals(FollowerClient.Kind.ACK,f.client.replicate("n2",entry(1)).kind());
            assertEquals(size,java.nio.file.Files.size(dir.resolve("wal")));
        }
    }
    @Test void realGrpcIndexGapAndConflictAreStructured()throws Exception {
        try(Fixture f=new Fixture(dir.resolve("wal"))){
            var gap=f.client.replicate("n2",entry(3));
            assertEquals(FollowerClient.Kind.INDEX_GAP,gap.kind());assertEquals(1,gap.expectedNextIndex());
            assertEquals(FollowerClient.Kind.ACK,f.client.replicate("n2",entry(1)).kind());
            var conflicting=new LogEntry(1,"other",entry(1).command(),entry(1).commandFingerprint());
            var conflict=f.client.replicate("n2",conflicting);
            assertEquals(FollowerClient.Kind.INDEX_CONFLICT,conflict.kind());assertEquals(2,conflict.expectedNextIndex());
            assertEquals(1,f.store.lastDurableLogIndex());
        }
    }
    @Test void realGrpcStatusReportsLatestDurableIndexNoCommitOrReadiness()throws Exception {
        try(Fixture f=new Fixture(dir.resolve("wal"))){
            f.client.replicate("n2",entry(1));
            ManagedChannel channel=ManagedChannelBuilder.forAddress("127.0.0.1",f.server.getPort()).usePlaintext().build();
            try {
                var response=ReplicationServiceGrpc.newBlockingStub(channel).withDeadlineAfter(3,TimeUnit.SECONDS)
                    .getReplicationStatus(ReplicationStatusRequest.newBuilder().setLeaderId("n1").build());
                assertEquals(1,response.getLastLogIndex());assertEquals(0,response.getCommitIndex());
                assertEquals(0,response.getAppliedIndex());assertFalse(response.getReady());
            }finally{channel.shutdownNow();}
        }
    }
    @Test void invalidAcceptedResponseFromWrongFollowerNeverCountsAsAck()throws Exception {
        Server impostor=NettyServerBuilder.forPort(0)
                .addService(new ReplicationServiceGrpc.ReplicationServiceImplBase(){
                    @Override public void replicateEntry(ReplicateEntryRequest request,StreamObserver<ReplicateEntryResponse> observer){
                        observer.onNext(ReplicateEntryResponse.newBuilder().setFollowerId("n3").setAccepted(true)
                                .setLastPersistedIndex(request.getEntry().getIndex()).build());observer.onCompleted();
                    }
                }).build().start();
        try(var client=new GrpcFollowerClient(ClusterConfigTest.config(dir.resolve("leader"),"n1"),
                followerId->new NodeEndpoint("127.0.0.1",impostor.getPort()))){
            assertEquals(FollowerClient.Kind.INVALID_RESPONSE,client.replicate("n2",entry(1)).kind());
        }finally{impostor.shutdownNow();assertTrue(impostor.awaitTermination(3,TimeUnit.SECONDS));}
    }
    @Test void acceptedResponseWithoutIndexCoverageIsRejected()throws Exception {
        Server impostor=NettyServerBuilder.forPort(0)
                .addService(new ReplicationServiceGrpc.ReplicationServiceImplBase(){
                    @Override public void replicateEntry(ReplicateEntryRequest request,StreamObserver<ReplicateEntryResponse> observer){
                        observer.onNext(ReplicateEntryResponse.newBuilder().setFollowerId("n2").setAccepted(true)
                                .setLastPersistedIndex(0).build());observer.onCompleted();
                    }
                }).build().start();
        try(var client=new GrpcFollowerClient(ClusterConfigTest.config(dir.resolve("leader"),"n1"),
                followerId->new NodeEndpoint("127.0.0.1",impostor.getPort()))){
            assertEquals(FollowerClient.Kind.INVALID_RESPONSE,client.replicate("n2",entry(1)).kind());
        }finally{impostor.shutdownNow();assertTrue(impostor.awaitTermination(3,TimeUnit.SECONDS));}
    }
    @Test void transportUnavailableIsNeverDurableAck()throws Exception {
        Server unavailable=NettyServerBuilder.forPort(0)
                .addService(new ReplicationServiceGrpc.ReplicationServiceImplBase(){
                    @Override public void replicateEntry(ReplicateEntryRequest request,StreamObserver<ReplicateEntryResponse> observer){
                        observer.onError(Status.UNAVAILABLE.withDescription("fixture disconnected").asRuntimeException());
                    }
                }).build().start();
        try(var client=new GrpcFollowerClient(ClusterConfigTest.config(dir.resolve("leader"),"n1"),
                followerId->new NodeEndpoint("127.0.0.1",unavailable.getPort()))){
            assertEquals(FollowerClient.Kind.UNAVAILABLE,client.replicate("n2",entry(1)).kind());
        }finally{unavailable.shutdownNow();assertTrue(unavailable.awaitTermination(3,TimeUnit.SECONDS));}
    }
    @Test void transportDeadlineIsUnknownOutcomeAndNotAck()throws Exception {
        Server nonReplying=NettyServerBuilder.forPort(0)
                .addService(new ReplicationServiceGrpc.ReplicationServiceImplBase(){
                    @Override public void replicateEntry(ReplicateEntryRequest request,StreamObserver<ReplicateEntryResponse> observer){
                        // Simulate an RPC accepted for processing whose ACK never arrives.
                    }
                }).build().start();
        try(var client=new GrpcFollowerClient(ClusterConfigTest.config(dir.resolve("leader"),"n1"),
                followerId->new NodeEndpoint("127.0.0.1",nonReplying.getPort()))){
            assertEquals(FollowerClient.Kind.DEADLINE_EXCEEDED,client.replicate("n2",entry(1)).kind());
        }finally{nonReplying.shutdownNow();assertTrue(nonReplying.awaitTermination(3,TimeUnit.SECONDS));}
    }
    @Test void manuallySeededLeaderReplicatesToTwoRealFollowersWithoutCommit()throws Exception {
        Path follower2=dir.resolve("n2/raftkv.wal"),follower3=dir.resolve("n3/raftkv.wal");
        Path leaderDir=dir.resolve("n1");
        java.nio.file.Files.createDirectories(leaderDir);
        try(Fixture n2=new Fixture(follower2,"n2"); Fixture n3=new Fixture(follower3,"n3");
            var owner=java.nio.channels.FileChannel.open(leaderDir.resolve("node.lock"),
                java.nio.file.StandardOpenOption.CREATE,java.nio.file.StandardOpenOption.WRITE);
            var lease=owner.lock();
            var leader=WalBackedPersistentLog.open(leaderDir.resolve("raftkv.wal"))){
            leader.append(entry(1)); // leader entry exists durably before any RPC
            var outbound=new GrpcFollowerClient(ClusterConfigTest.config(leaderDir,"n1"),
                followerId->new NodeEndpoint("127.0.0.1",followerId.equals("n2")?n2.server.getPort():n3.server.getPort()));
            try(ReplicationManager manager=new ReplicationManager(ClusterConfigTest.config(leaderDir,"n1"),leader,outbound)){
                var result=manager.replicateStoredEntry(1).outcomes();
                assertEquals(FollowerClient.Kind.ACK,result.get("n2").kind());
                assertEquals(FollowerClient.Kind.ACK,result.get("n3").kind());
                assertEquals(1,manager.highestAcceptedRequestIndex("n2"));
                assertEquals(1,manager.highestAcceptedRequestIndex("n3"));
                assertEquals(entry(1),n2.log.get(1).orElseThrow());
                assertEquals(entry(1),n3.log.get(1).orElseThrow());
                assertEquals(0,n2.store.durableCommitIndex());
                assertEquals(0,n3.store.durableCommitIndex());
                assertFalse(n2.snapshot.get().ready());
                assertFalse(n3.snapshot.get().ready());
            }
        }
        try(var p2=WalBackedPersistentLog.open(follower2);
            var p3=WalBackedPersistentLog.open(follower3)){
            assertEquals(entry(1),p2.get(1).orElseThrow());
            assertEquals(entry(1),p3.get(1).orElseThrow());
        }
    }

}
