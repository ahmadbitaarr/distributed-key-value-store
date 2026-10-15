package io.github.ahmadbitaarr.raftkv.replication;

import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.server.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class FollowerReplicationHandlerTest {
    @TempDir Path dir;
    private static LogEntry entry(long index){return new LogEntry(index,"r"+index,Command.put("key",new byte[]{0,-1,3}),new byte[]{4,5});}
    private static LogEntry altered(LogEntry old){return new LogEntry(old.index(),old.requestId(),Command.put("key",new byte[]{42}),old.commandFingerprint());}
    private static final class Rig implements AutoCloseable {
        final WalStore store; final WalBackedPersistentLog log; final ReplicationOperationGate gate=new ReplicationOperationGate();
        final AtomicReference<NodeStatusSnapshot> snapshot=new AtomicReference<>(new NodeStatusSnapshot("n2",NodeRole.FOLLOWER,NodeLifecycleState.CONTROL_ONLY,0,0,0,false));
        final AtomicReference<IOException> fault=new AtomicReference<>();
        final FollowerReplicationHandler handler;
        Rig(WalStore store, Path walDirectory){
            this.store=store;log=new WalBackedPersistentLog(store);
            snapshot.set(new NodeStatusSnapshot("n2",NodeRole.FOLLOWER,NodeLifecycleState.CONTROL_ONLY,log.lastLogIndex(),store.durableCommitIndex(),0,false));
            handler=new FollowerReplicationHandler(ClusterConfigTest.config(walDirectory,"n2"),log,store,gate,snapshot::get,
                index->snapshot.updateAndGet(s->new NodeStatusSnapshot("n2",NodeRole.FOLLOWER,s.state(),index,s.durableCommitIndex(),0,false)),
                error->{fault.compareAndSet(null,error);snapshot.updateAndGet(s->s.withState(NodeLifecycleState.FAILED));});
            gate.open();
        }
        public void close()throws IOException {gate.closeAdmission();log.close();}
    }
    private Rig open() throws Exception {return new Rig(FileWalStore.open(dir.resolve("wal")),dir);}
    @Test void firstContiguousEntryIsDurablyAckedAndReopens()throws Exception {
        LogEntry entry=entry(1);
        try(Rig rig=open()){
            var result=rig.handler.replicate("n1",entry);assertTrue(result.accepted());
            assertEquals(1,rig.snapshot.get().lastLogIndex());assertEquals(0,rig.snapshot.get().durableCommitIndex());
            assertFalse(rig.snapshot.get().ready()); assertEquals(0,rig.snapshot.get().appliedIndex());
        }
        try(var recovered=WalBackedPersistentLog.open(dir.resolve("wal"))){assertEquals(entry,recovered.get(1).orElseThrow());}
    }
    @Test void nextEntryAckedAndDuplicateIdempotent() throws Exception {
        try(Rig rig=open()){
            assertTrue(rig.handler.replicate("n1",entry(1)).accepted());
            assertTrue(rig.handler.replicate("n1",entry(2)).accepted());
            assertTrue(rig.handler.replicate("n1",entry(1)).accepted());
            assertEquals(2,rig.store.lastDurableLogIndex());assertEquals(2,rig.snapshot.get().lastLogIndex());
        }
    }
    @Test void indexGapReturnsExpectedNextIndexAndDoesNotWrite()throws Exception {
        try(Rig rig=open()){
            var result=rig.handler.replicate("n1",entry(4));
            assertEquals(FollowerReplicationHandler.Kind.INDEX_GAP,result.kind());
            assertEquals(1,result.expectedNextIndex());assertEquals(0,rig.store.lastDurableLogIndex());
        }
    }
    @Test void indexConflictNeverOverwritesStoredEntry()throws Exception {
        try(Rig rig=open()){
            rig.handler.replicate("n1",entry(1));
            assertEquals(FollowerReplicationHandler.Kind.INDEX_CONFLICT,rig.handler.replicate("n1",altered(entry(1))).kind());
            assertEquals(entry(1),rig.log.get(1).orElseThrow());
        }
    }
    @Test void exactEntryIdentityComparesRequestIdAndFingerprint()throws Exception {
        try(Rig rig=open()){
            rig.handler.replicate("n1",entry(1));
            assertEquals(FollowerReplicationHandler.Kind.INDEX_CONFLICT,rig.handler.replicate("n1",new LogEntry(1,"other",entry(1).command(),entry(1).commandFingerprint())).kind());
            assertEquals(FollowerReplicationHandler.Kind.INDEX_CONFLICT,rig.handler.replicate("n1",new LogEntry(1,"r1",entry(1).command(),new byte[]{99})).kind());
        }
    }
    @Test void failedAppendNeverPublishesIndexAndBlocksFurtherReplication()throws Exception {
        WalStore real=FileWalStore.open(dir.resolve("wal"));
        var broken=new ForwardingWal(real){@Override public void appendEntryDurably(LogEntry e)throws IOException{throw new IOException("simulated uncertain force result");}};
        try(Rig rig=new Rig(broken,dir)){
            assertThrows(IOException.class,()->rig.handler.replicate("n1",entry(1)));
            assertEquals(NodeLifecycleState.FAILED,rig.snapshot.get().state());
            assertEquals(0,rig.snapshot.get().lastLogIndex());
            assertNotNull(rig.fault.get());
            assertThrows(FollowerReplicationHandler.StorageFailedException.class,()->rig.handler.replicate("n1",entry(1)));
            assertEquals(0,real.lastDurableLogIndex());
        }
    }
    @Test void durableAckCannotPrecedeBlockedAppendReturn()throws Exception {
        WalStore real=FileWalStore.open(dir.resolve("wal"));
        CountDownLatch entered=new CountDownLatch(1), release=new CountDownLatch(1);
        var delayed=new ForwardingWal(real){@Override public void appendEntryDurably(LogEntry e)throws IOException{
            entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new IOException("fixture timeout");}
            catch(InterruptedException x){Thread.currentThread().interrupt();throw new IOException(x);}super.appendEntryDurably(e);
        }};
        try(Rig rig=new Rig(delayed,dir)){
            var executor=Executors.newSingleThreadExecutor();
            try {
                Future<FollowerReplicationHandler.Decision> result=executor.submit(()->rig.handler.replicate("n1",entry(1)));
                assertTrue(entered.await(2,TimeUnit.SECONDS));
                assertEquals(0,rig.snapshot.get().lastLogIndex());
                assertEquals(1,rig.gate.activeCount());
                assertFalse(result.isDone());
                release.countDown();
                assertTrue(result.get(3,TimeUnit.SECONDS).accepted());
                assertEquals(1,rig.snapshot.get().lastLogIndex());
            }finally{release.countDown();executor.shutdownNow();}
        }
    }
    @Test void observerFailureAfterDurableAppendNeverDuplicatesWalFrame() throws Exception {
        try (Rig rig = open()) {
            var service = new io.github.ahmadbitaarr.raftkv.rpc.ReplicationGrpcService(
                    ClusterConfigTest.config(dir, "n2"), rig.snapshot::get, rig.handler);
            var request = io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicateEntryRequest
                    .newBuilder()
                    .setLeaderId("n1")
                    .setEntry(io.github.ahmadbitaarr.raftkv.rpc.ProtoMapper.toProtoLogEntry(entry(1)))
                    .build();

            AtomicInteger next = new AtomicInteger();
            AtomicInteger errors = new AtomicInteger();
            AtomicInteger completed = new AtomicInteger();

            var throwing = new io.grpc.stub.StreamObserver<
                    io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicateEntryResponse>() {
                @Override public void onNext(
                        io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicateEntryResponse value) {
                    next.incrementAndGet();
                    assertTrue(value.getAccepted());
                    throw new IllegalStateException("observer failed after durable append");
                }
                @Override public void onError(Throwable error) {
                    errors.incrementAndGet();
                }
                @Override public void onCompleted() {
                    completed.incrementAndGet();
                }
            };

            assertThrows(IllegalStateException.class,
                    () -> service.replicateEntry(request, throwing));

            assertEquals(1, next.get());
            assertEquals(0, errors.get());
            assertEquals(0, completed.get());
            assertEquals(1, rig.store.lastDurableLogIndex());
            assertEquals(entry(1), rig.log.get(1).orElseThrow());

            long size = java.nio.file.Files.size(dir.resolve("wal"));
            AtomicReference<
                    io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicateEntryResponse>
                    retryResponse = new AtomicReference<>();
            AtomicInteger retryCompletions = new AtomicInteger();

            service.replicateEntry(request, new io.grpc.stub.StreamObserver<>() {
                @Override public void onNext(
                        io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicateEntryResponse value) {
                    retryResponse.set(value);
                }
                @Override public void onError(Throwable error) {
                    fail("retry must not fail", error);
                }
                @Override public void onCompleted() {
                    retryCompletions.incrementAndGet();
                }
            });

            assertNotNull(retryResponse.get());
            assertTrue(retryResponse.get().getAccepted());
            assertEquals(1, retryCompletions.get());
            assertEquals(1, rig.store.lastDurableLogIndex());
            assertEquals(size, java.nio.file.Files.size(dir.resolve("wal")));
        }
    }

    private static class ForwardingWal implements WalStore {
        private final WalStore base;ForwardingWal(WalStore base){this.base=base;}
        public WalRecoveryResult recoveryResult(){return base.recoveryResult();}
        public void appendEntryDurably(LogEntry e)throws IOException{base.appendEntryDurably(e);}
        public void appendCommitDurably(long i)throws IOException{base.appendCommitDurably(i);}
        public void rewriteDurably(List<LogEntry> e,long i)throws IOException{base.rewriteDurably(e,i);}
        public long lastDurableLogIndex(){return base.lastDurableLogIndex();}
        public long durableCommitIndex(){return base.durableCommitIndex();}
        public void close()throws IOException{base.close();}
    }
    @Test void wrongLeaderIdentityRejectedAtDomainBoundary() throws Exception {
        try(Rig rig=open()){
            assertThrows(FollowerReplicationHandler.UnrecognizedLeaderException.class,
                    ()->rig.handler.replicate("n3", entry(1)));
            assertEquals(0,rig.store.lastDurableLogIndex());
        }
    }

    @Test void identicalAlreadyCommittedEntryAcknowledgedWithoutRewriting()throws Exception {
        try(var seed=FileWalStore.open(dir.resolve("wal"))){
            seed.appendEntryDurably(entry(1));
            seed.appendCommitDurably(1);
        }
        long size=java.nio.file.Files.size(dir.resolve("wal"));
        try(Rig rig=open()){
            var result=rig.handler.replicate("n1",entry(1));
            assertTrue(result.accepted());
            assertEquals(1,result.commitIndex());
            assertEquals(size,java.nio.file.Files.size(dir.resolve("wal")));
        }
    }
    @Test void concurrentSameIndexRequestsSerializeAndNeverDuplicateWalFrame()throws Exception {
        try(Rig rig=open()){
            CountDownLatch start=new CountDownLatch(1);
            ExecutorService executor=Executors.newFixedThreadPool(2);
            try {
                var a=executor.submit(()->{start.await();return rig.handler.replicate("n1",entry(1));});
                var b=executor.submit(()->{start.await();return rig.handler.replicate("n1",entry(1));});
                start.countDown();
                assertTrue(a.get(4,TimeUnit.SECONDS).accepted());
                assertTrue(b.get(4,TimeUnit.SECONDS).accepted());
                assertEquals(1,rig.store.lastDurableLogIndex());
            }finally{start.countDown();executor.shutdownNow();}
        }
    }
    @Test void unexpectedPostForcePublicationFailureFailsClosedEvenIfDiskHasEntry()throws Exception {
        WalStore store=FileWalStore.open(dir.resolve("wal"));
        ReplicationOperationGate gate=new ReplicationOperationGate();gate.open();
        AtomicReference<NodeStatusSnapshot> snapshot=new AtomicReference<>(new NodeStatusSnapshot("n2",NodeRole.FOLLOWER,NodeLifecycleState.CONTROL_ONLY,0,0,0,false));
        AtomicReference<IOException> firstFault=new AtomicReference<>();
        try(var log=new WalBackedPersistentLog(store)){
            FollowerReplicationHandler handler=new FollowerReplicationHandler(ClusterConfigTest.config(dir,"n2"),
                log,store,gate,snapshot::get,n->{throw new IllegalStateException("simulated publication failure");},
                io->{firstFault.compareAndSet(null,io);snapshot.updateAndGet(s->s.withState(NodeLifecycleState.FAILED));});
            assertThrows(IOException.class,()->handler.replicate("n1",entry(1)));
            assertNotNull(firstFault.get());
            assertEquals(0,snapshot.get().lastLogIndex());
            assertEquals(NodeLifecycleState.FAILED,snapshot.get().state());
            assertThrows(FollowerReplicationHandler.StorageFailedException.class,()->handler.replicate("n1",entry(2)));
        }
        try(var reopen=WalBackedPersistentLog.open(dir.resolve("wal"))){
            assertEquals(entry(1),reopen.get(1).orElseThrow());
        }
    }

    @Test void confirmedWalCorruptionMapsToDataLossAndClosesReplicationAdmission() throws Exception {
        WalStore real=FileWalStore.open(dir.resolve("wal"));
        var corrupt=new ForwardingWal(real){@Override public void appendEntryDurably(LogEntry e)throws IOException{
            throw new WalCorruptionException("confirmed invalid WAL record");
        }};
        try(Rig rig=new Rig(corrupt,dir)){
            var service=new io.github.ahmadbitaarr.raftkv.rpc.ReplicationGrpcService(
                    ClusterConfigTest.config(dir,"n2"),rig.snapshot::get,rig.handler);
            var request=io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicateEntryRequest.newBuilder()
                    .setLeaderId("n1")
                    .setEntry(io.github.ahmadbitaarr.raftkv.rpc.ProtoMapper.toProtoLogEntry(entry(1))).build();
            var first=new AtomicReference<Throwable>();
            var afterFailure=new AtomicReference<Throwable>();
            service.replicateEntry(request,captureError(first));
            assertEquals(io.grpc.Status.Code.DATA_LOSS,io.grpc.Status.fromThrowable(first.get()).getCode());
            assertEquals(NodeLifecycleState.FAILED,rig.snapshot.get().state());
            assertEquals(0,rig.snapshot.get().lastLogIndex());
            assertNull(rig.gate.tryAcquire(),"failure must close admission");
            service.replicateEntry(request,captureError(afterFailure));
            assertEquals(io.grpc.Status.Code.DATA_LOSS,io.grpc.Status.fromThrowable(afterFailure.get()).getCode());
            assertEquals(0,real.lastDurableLogIndex());
        }
    }

    @Test void genericWalIoErrorMapsToInternalButStillFailsClosed() throws Exception {
        WalStore real=FileWalStore.open(dir.resolve("wal"));
        var broken=new ForwardingWal(real){@Override public void appendEntryDurably(LogEntry e)throws IOException{
            throw new IOException("unknown durable state");
        }};
        try(Rig rig=new Rig(broken,dir)){
            var service=new io.github.ahmadbitaarr.raftkv.rpc.ReplicationGrpcService(
                    ClusterConfigTest.config(dir,"n2"),rig.snapshot::get,rig.handler);
            var request=io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicateEntryRequest.newBuilder()
                    .setLeaderId("n1")
                    .setEntry(io.github.ahmadbitaarr.raftkv.rpc.ProtoMapper.toProtoLogEntry(entry(1))).build();
            var error=new AtomicReference<Throwable>();
            service.replicateEntry(request,captureError(error));
            assertEquals(io.grpc.Status.Code.INTERNAL,io.grpc.Status.fromThrowable(error.get()).getCode());
            assertEquals(NodeLifecycleState.FAILED,rig.snapshot.get().state());
            assertNull(rig.gate.tryAcquire());
        }
    }

    private static io.grpc.stub.StreamObserver<io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicateEntryResponse>
            captureError(AtomicReference<Throwable> captured) {
        return new io.grpc.stub.StreamObserver<>() {
            @Override public void onNext(io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicateEntryResponse value) {
                fail("failed WAL operation must never ACK");
            }
            @Override public void onError(Throwable error) {assertNull(captured.getAndSet(error),"duplicate terminal signal");}
            @Override public void onCompleted() {fail("failed WAL operation must not complete");}
        };
    }

}
