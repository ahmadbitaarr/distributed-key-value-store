package io.github.ahmadbitaarr.raftkv.rpc;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.inprocess.*;
import io.grpc.stub.StreamObserver;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.log.WalBackedPersistentLog;
import io.github.ahmadbitaarr.raftkv.replication.*;
import io.github.ahmadbitaarr.raftkv.server.*;
import io.github.ahmadbitaarr.raftkv.statemachine.*;
import io.github.ahmadbitaarr.raftkv.wal.FileWalStore;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class Phase9MaintenanceGrpcTest {
    @TempDir Path directory;
    private final class Rig implements AutoCloseable {
        final FileWalStore wal;
        final WalBackedPersistentLog log;
        final StateMachineApplier applier=new StateMachineApplier(new KvStateMachine());
        final ReplicationOperationGate gate=new ReplicationOperationGate();
        final AtomicReference<NodeStatusSnapshot> snapshot=new AtomicReference<>(new NodeStatusSnapshot("n2",
                io.github.ahmadbitaarr.raftkv.server.NodeRole.FOLLOWER,NodeLifecycleState.CONTROL_ONLY,0,0,0,false));
        final ReplicationGrpcService service;
        final Server server;
        final ManagedChannel channel;
        final ReplicationServiceGrpc.ReplicationServiceBlockingStub stub;
        Rig()throws Exception{
            var config=ClusterConfigTest.config(directory,"n2");
            wal=FileWalStore.open(config.walFile());log=new WalBackedPersistentLog(wal);
            var handler=new FollowerReplicationHandler(config,log,wal,gate,snapshot::get,
                    n->publish(n,snapshot.get().durableCommitIndex(),snapshot.get().appliedIndex()),applier,
                    n->publish(snapshot.get().lastLogIndex(),n,snapshot.get().appliedIndex()),
                    n->publish(snapshot.get().lastLogIndex(),snapshot.get().durableCommitIndex(),n),
                    e->snapshot.updateAndGet(o->o.withState(NodeLifecycleState.FAILED)));
            gate.open();service=new ReplicationGrpcService(config,snapshot::get,handler);
            var name=InProcessServerBuilder.generateName();
            server=InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start();
            channel=InProcessChannelBuilder.forName(name).directExecutor().build();
            stub=ReplicationServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5,TimeUnit.SECONDS);
        }
        void publish(long last,long committed,long applied){
            snapshot.set(new NodeStatusSnapshot("n2",io.github.ahmadbitaarr.raftkv.server.NodeRole.FOLLOWER,
                    NodeLifecycleState.CONTROL_ONLY,last,committed,applied,false));
        }
        ReplicateEntryRequest entry(int index){return ReplicateEntryRequest.newBuilder().setLeaderId("n1")
                .setEntry(io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.LogEntry.newBuilder()
                .setIndex(index).setRequestId("id-"+index).setOperation(OperationType.OPERATION_TYPE_PUT)
                .setKey("k").setValue(ByteString.copyFrom(new byte[]{(byte)index}))
                .setCommandFingerprint(ByteString.copyFrom(new byte[]{(byte)index})).build()).build();}
        AdvanceCommitRequest commit(long n){return AdvanceCommitRequest.newBuilder().setLeaderId("n1").setCommitIndex(n).build();}
        TruncateSuffixRequest truncate(long n){return TruncateSuffixRequest.newBuilder().setLeaderId("n1").setTruncateAfterIndex(n).build();}
        @Override public void close()throws Exception {channel.shutdownNow();server.shutdownNow();log.close();}
    }
    @Test void forcedCommitReportsAppliedProgressOverGrpc()throws Exception{
        try(var rig=new Rig()){
            assertTrue(rig.stub.replicateEntry(rig.entry(1)).getAccepted());
            assertEquals(1,rig.stub.advanceCommit(rig.commit(1)).getCommitIndex());
            assertEquals(1,rig.stub.advanceCommit(rig.commit(1)).getAppliedIndex());
            assertEquals(1,rig.stub.getReplicationStatus(ReplicationStatusRequest.newBuilder().setLeaderId("n1").build()).getAppliedIndex());
        }
    }
    @Test void invalidCommitTargetRejectedWithFailedPrecondition()throws Exception{
        try(var rig=new Rig()){
            assertEquals(Status.Code.FAILED_PRECONDITION,assertThrows(StatusRuntimeException.class,
                    ()->rig.stub.advanceCommit(rig.commit(1))).getStatus().getCode());
            assertEquals(0,rig.wal.durableCommitIndex());
        }
    }
    @Test void wrongLeaderIdentityRejectedForBothMaintenanceRpcs()throws Exception{
        try(var rig=new Rig()){
            var wrongCommit=AdvanceCommitRequest.newBuilder().setLeaderId("n3").setCommitIndex(0).build();
            assertEquals(Status.Code.FAILED_PRECONDITION,assertThrows(StatusRuntimeException.class,
                    ()->rig.stub.advanceCommit(wrongCommit)).getStatus().getCode());
            var wrongTruncate=TruncateSuffixRequest.newBuilder().setLeaderId("n3").setTruncateAfterIndex(0).build();
            assertEquals(Status.Code.FAILED_PRECONDITION,assertThrows(StatusRuntimeException.class,
                    ()->rig.stub.truncateUncommittedSuffix(wrongTruncate)).getStatus().getCode());
        }
    }
    @Test void malformedMaintenanceRequestReturnsInvalidArgument()throws Exception{
        try(var rig=new Rig()){
            assertEquals(Status.Code.INVALID_ARGUMENT,assertThrows(StatusRuntimeException.class,
                    ()->rig.stub.advanceCommit(AdvanceCommitRequest.getDefaultInstance())).getStatus().getCode());
            assertEquals(Status.Code.INVALID_ARGUMENT,assertThrows(StatusRuntimeException.class,
                    ()->rig.stub.truncateUncommittedSuffix(TruncateSuffixRequest.getDefaultInstance())).getStatus().getCode());
        }
    }
    @Test void truncationCannotRemoveCommittedEntryAndValidRewriteIsDurable()throws Exception{
        try(var rig=new Rig()){
            rig.stub.replicateEntry(rig.entry(1));rig.stub.replicateEntry(rig.entry(2));
            rig.stub.advanceCommit(rig.commit(1));
            assertEquals(Status.Code.FAILED_PRECONDITION,assertThrows(StatusRuntimeException.class,
                    ()->rig.stub.truncateUncommittedSuffix(rig.truncate(0))).getStatus().getCode());
            assertEquals(1,rig.stub.truncateUncommittedSuffix(rig.truncate(1)).getLastLogIndex());
            assertEquals(1,rig.wal.lastDurableLogIndex());assertEquals(1,rig.wal.durableCommitIndex());
        }
    }
    @Test void successfulObserverCompletionFailureDoesNotCauseDuplicateCommitCallback()throws Exception{
        try(var rig=new Rig()){
            rig.stub.replicateEntry(rig.entry(1));
            var errors=new AtomicInteger();var completed=new AtomicInteger();var next=new AtomicInteger();
            var thrown=new IllegalArgumentException("client callback failure, not storage error");
            StreamObserver<AdvanceCommitResponse> observer=new StreamObserver<>(){
                public void onNext(AdvanceCommitResponse response){next.incrementAndGet();}
                public void onError(Throwable error){errors.incrementAndGet();}
                public void onCompleted(){completed.incrementAndGet();throw thrown;}
            };
            assertSame(thrown,assertThrows(IllegalArgumentException.class,
                    ()->rig.service.advanceCommit(rig.commit(1),observer)));
            assertEquals(1,next.get());assertEquals(1,completed.get());assertEquals(0,errors.get());
            assertEquals(1,rig.wal.durableCommitIndex());
        }
    }
}
