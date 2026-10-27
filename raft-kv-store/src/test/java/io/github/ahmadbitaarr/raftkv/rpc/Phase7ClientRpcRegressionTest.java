package io.github.ahmadbitaarr.raftkv.rpc;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.github.ahmadbitaarr.raftkv.commit.*;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.leader.LeaderCoordinator;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.replication.*;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.server.*;
import io.github.ahmadbitaarr.raftkv.statemachine.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class Phase7ClientRpcRegressionTest {
    @TempDir Path dir;
    static final class Recorder implements StreamObserver<MutationResponse> {
        MutationResponse next;Throwable error;int completed,errors;
        @Override public void onNext(MutationResponse result){next=result;}
        @Override public void onError(Throwable throwable){error=throwable;errors++;}
        @Override public void onCompleted(){completed++;}
    }
    static final class FailingWal implements WalStore {
        final FileWalStore delegate;
        boolean corruption;
        FailingWal(Path path)throws IOException{delegate=FileWalStore.open(path);}
        @Override public WalRecoveryResult recoveryResult(){return delegate.recoveryResult();}
        @Override public void appendEntryDurably(LogEntry e)throws IOException{
            if(corruption)throw new WalCorruptionException("injected confirmed corruption");
            delegate.appendEntryDurably(e);
        }
        @Override public void appendCommitDurably(long n)throws IOException{delegate.appendCommitDurably(n);}
        @Override public void rewriteDurably(List<LogEntry> e,long n)throws IOException{delegate.rewriteDurably(e,n);}
        @Override public long lastDurableLogIndex(){return delegate.lastDurableLogIndex();}
        @Override public long durableCommitIndex(){return delegate.durableCommitIndex();}
        @Override public void close()throws IOException{delegate.close();}
    }
    private static PutRequest put(String id){return PutRequest.newBuilder().setRequestId(id)
            .setKey("x").setValue(ByteString.copyFrom(new byte[]{1})).build();}
    private static NodeStatusSnapshot readyForControl(){return new NodeStatusSnapshot("n1",NodeRole.LEADER,
            NodeLifecycleState.CONTROL_ONLY,0,0,0,false);}

    @Test void observerFailureAfterCommitDoesNotReapplyOrSignalError()throws Exception {
        var cfg=ClusterConfigTest.config(dir,"n1");
        try(var wal=FileWalStore.open(cfg.walFile())) {
            var log=new WalBackedPersistentLog(wal);
            FollowerClient client=(id,e)->id.equals("n2")?FollowerClient.ack(e.index(),e.index()):
                    new FollowerClient.Outcome(FollowerClient.Kind.UNAVAILABLE,e.index(),0,0,"down");
            try(var manager=new ReplicationManager(cfg,log,client)) {
                var tracker=new AckTracker(cfg,log,wal);
                var coordinator=new LeaderCoordinator(cfg,log,wal,manager,
                        id->Optional.of(new FollowerStatusProbe.Status(id,NodeRole.FOLLOWER,0,0,0,false)),
                        tracker,new CommitManager(tracker,wal,log,n->{}),new StateMachineApplier(new KvStateMachine()),
                        n->{},n->{},e->{});
                coordinator.openAfterStartup();
                var service=new ClientKvGrpcService(cfg,Phase7ClientRpcRegressionTest::readyForControl,coordinator);
                var callbackCalls=new AtomicInteger();
                StreamObserver<MutationResponse> broken=new StreamObserver<>() {
                    @Override public void onNext(MutationResponse result){callbackCalls.incrementAndGet();throw new IllegalStateException("client gone");}
                    @Override public void onError(Throwable error){callbackCalls.addAndGet(100);}
                    @Override public void onCompleted(){callbackCalls.addAndGet(1000);}
                };
                assertThrows(IllegalStateException.class,()->service.put(put("once"),broken));
                assertEquals(1,callbackCalls.get());
                assertEquals(1,wal.durableCommitIndex());
                var second=new Recorder();service.put(put("once"),second);
                assertEquals(ResultCode.RESULT_CODE_OK,second.next.getCode()); // Phase 11 replays committed result after lost response
                assertEquals(1,second.next.getCommitIndex());
                assertEquals(1,log.lastLogIndex());assertEquals(1,wal.durableCommitIndex());
            }
        }
    }
    @Test void confirmedLeaderWalCorruptionUsesDataLossNotInternal()throws Exception {
        var cfg=ClusterConfigTest.config(dir,"n1");
        try(var wal=new FailingWal(cfg.walFile())) {
            var log=new WalBackedPersistentLog(wal);
            try(var manager=new ReplicationManager(cfg,log,(id,e)->FollowerClient.ack(e.index(),e.index()))) {
                var tracker=new AckTracker(cfg,log,wal);
                var firstFault=new AtomicReference<IOException>();
                var coordinator=new LeaderCoordinator(cfg,log,wal,manager,
                        id->Optional.of(new FollowerStatusProbe.Status(id,NodeRole.FOLLOWER,0,0,0,false)),
                        tracker,new CommitManager(tracker,wal,log,n->{}),new StateMachineApplier(new KvStateMachine()),
                        n->{},n->{},firstFault::set);
                coordinator.openAfterStartup();wal.corruption=true;
                var service=new ClientKvGrpcService(cfg,Phase7ClientRpcRegressionTest::readyForControl,coordinator);
                var recorder=new Recorder();service.put(put("corruption"),recorder);
                assertEquals(Status.Code.DATA_LOSS,Status.fromThrowable(recorder.error).getCode());
                assertEquals(1,recorder.errors);assertEquals(0,recorder.completed);
                assertTrue(firstFault.get() instanceof WalCorruptionException);
                var blocked=new Recorder();service.put(put("later"),blocked);
                assertEquals(ResultCode.RESULT_CODE_NOT_READY,blocked.next.getCode());
                assertEquals(0,log.lastLogIndex());
            }
        }
    }
}
