package io.github.ahmadbitaarr.raftkv.replication;

import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.server.*;
import io.github.ahmadbitaarr.raftkv.statemachine.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

class FollowerMaintenancePhase9Test {
    @TempDir Path directory;
    private static LogEntry entry(long index,int value){
        return new LogEntry(index,"req-"+index,Command.put("key"+index,new byte[]{(byte)value}),new byte[]{(byte)value});
    }

    private static final class Decorator implements WalStore {
        final WalStore real;
        boolean failAfterCommit,failBeforeCommit,failBeforeRewrite;
        Decorator(WalStore real){this.real=real;}
        public WalRecoveryResult recoveryResult(){return real.recoveryResult();}
        public void appendEntryDurably(LogEntry e)throws IOException{real.appendEntryDurably(e);}
        public void appendCommitDurably(long n)throws IOException{
            if(failBeforeCommit)throw new IOException("COMMIT force failed before persistence");
            real.appendCommitDurably(n);
            if(failAfterCommit)throw new IOException("post-force COMMIT outcome unknown");
        }
        public void rewriteDurably(List<LogEntry> e,long n)throws IOException{
            if(failBeforeRewrite)throw new IOException("rewrite force failed");
            real.rewriteDurably(e,n);
        }
        public long lastDurableLogIndex(){return real.lastDurableLogIndex();}
        public long durableCommitIndex(){return real.durableCommitIndex();}
        public void close()throws IOException{real.close();}
    }

    private final class Fixture implements AutoCloseable {
        final Path path=directory.resolve("follower/raftkv.wal");
        final Decorator wal;
        final WalBackedPersistentLog log;
        final ReplicationOperationGate gate=new ReplicationOperationGate();
        final StateMachineApplier applier=new StateMachineApplier(new KvStateMachine());
        final AtomicReference<NodeStatusSnapshot> status;
        final AtomicReference<IOException> failure=new AtomicReference<>();
        final FollowerReplicationHandler handler;
        Fixture()throws Exception {
            wal=new Decorator(FileWalStore.open(path));log=new WalBackedPersistentLog(wal);
            long committed=wal.durableCommitIndex();
            applier.applyThrough(committed,committed,log);
            status=new AtomicReference<>(new NodeStatusSnapshot("n2",NodeRole.FOLLOWER,
                    NodeLifecycleState.CONTROL_ONLY,log.lastLogIndex(),committed,committed,false));
            handler=new FollowerReplicationHandler(ClusterConfigTest.config(directory,"n2"),log,wal,gate,
                status::get,
                n->publish(n,status.get().durableCommitIndex(),status.get().appliedIndex()),applier,
                n->publish(status.get().lastLogIndex(),n,status.get().appliedIndex()),
                n->publish(status.get().lastLogIndex(),status.get().durableCommitIndex(),n),failure::set);
            gate.open();
        }
        void publish(long last,long committed,long applied){status.set(new NodeStatusSnapshot("n2",NodeRole.FOLLOWER,
                NodeLifecycleState.CONTROL_ONLY,last,committed,applied,false));}
        void put(long index,int value)throws Exception{
            assertTrue(handler.replicate("n1",entry(index,value)).accepted());
        }
        @Override public void close()throws Exception{gate.closeAdmission();log.close();}
    }

    @Test void forcedCommitAppliesExactlyOnceAndPublishesBothIndexes()throws Exception{
        try(var f=new Fixture()){
            f.put(1,11);f.put(2,22);
            assertEquals(0,f.applier.appliedIndex());
            assertEquals(2,f.handler.advanceCommit("n1",2).commitIndex());
            assertEquals(2,f.applier.appliedIndex());
            assertEquals(2,f.status.get().durableCommitIndex());
            assertEquals(2,f.status.get().appliedIndex());
            assertEquals(2,f.handler.advanceCommit("n1",2).appliedIndex());
            assertArrayEquals(new byte[]{22},f.applier.diagnosticGetForTest("key2").orElseThrow());
        }
    }
    @Test void commitBeyondDurableLogRejectedWithoutMutation()throws Exception{
        try(var f=new Fixture()){
            f.put(1,1);
            assertThrows(FollowerReplicationHandler.ProtocolRejectedException.class,()->f.handler.advanceCommit("n1",2));
            assertEquals(0,f.wal.durableCommitIndex());assertEquals(0,f.applier.appliedIndex());
        }
    }
    @Test void olderCommitIndexCannotRegress()throws Exception{
        try(var f=new Fixture()){
            f.put(1,1);f.put(2,2);f.handler.advanceCommit("n1",2);
            assertThrows(FollowerReplicationHandler.ProtocolRejectedException.class,()->f.handler.advanceCommit("n1",1));
            assertEquals(2,f.status.get().durableCommitIndex());assertEquals(2,f.applier.appliedIndex());
        }
    }
    @Test void wrongLeaderCannotAdvanceOrTruncate()throws Exception{
        try(var f=new Fixture()){
            f.put(1,1);
            assertThrows(FollowerReplicationHandler.UnrecognizedLeaderException.class,()->f.handler.advanceCommit("n3",1));
            assertThrows(FollowerReplicationHandler.UnrecognizedLeaderException.class,()->f.handler.truncateUncommittedSuffix("n3",0));
            assertEquals(1,f.log.lastLogIndex());assertEquals(0,f.wal.durableCommitIndex());
        }
    }
    @Test void safeUncommittedSuffixTruncatesDurablyAndIdempotently()throws Exception{
        try(var f=new Fixture()){
            f.put(1,1);f.put(2,2);f.handler.advanceCommit("n1",1);
            assertEquals(1,f.handler.truncateUncommittedSuffix("n1",1).lastLogIndex());
            assertEquals(1,f.handler.truncateUncommittedSuffix("n1",1).lastLogIndex());
            assertEquals(1,f.wal.lastDurableLogIndex());assertEquals(1,f.applier.appliedIndex());
        }
        try(var reopened=FileWalStore.open(directory.resolve("follower/raftkv.wal"))){
            assertEquals(1,reopened.lastDurableLogIndex());assertEquals(1,reopened.durableCommitIndex());
        }
    }
    @Test void committedOrAppliedPrefixCannotBeTruncated()throws Exception{
        try(var f=new Fixture()){
            f.put(1,1);f.put(2,2);f.handler.advanceCommit("n1",2);
            assertThrows(FollowerReplicationHandler.ProtocolRejectedException.class,
                    ()->f.handler.truncateUncommittedSuffix("n1",1));
            assertEquals(2,f.log.lastLogIndex());assertEquals(2,f.status.get().durableCommitIndex());
        }
    }
    @Test void truncationBeyondStoredPrefixRejected()throws Exception{
        try(var f=new Fixture()){
            assertThrows(FollowerReplicationHandler.ProtocolRejectedException.class,
                    ()->f.handler.truncateUncommittedSuffix("n1",1));
            assertEquals(0,f.log.lastLogIndex());
        }
    }
    @Test void duplicateCommittedEntryAcknowledgedWithoutReappend()throws Exception{
        try(var f=new Fixture()){
            f.put(1,1);f.handler.advanceCommit("n1",1);
            assertTrue(f.handler.replicate("n1",entry(1,1)).accepted());
            assertEquals(1,f.log.lastLogIndex());assertEquals(1,f.applier.appliedIndex());
        }
    }
    @Test void committedEntryConflictNeverOverwritten()throws Exception{
        try(var f=new Fixture()){
            f.put(1,1);f.handler.advanceCommit("n1",1);
            assertEquals(FollowerReplicationHandler.Kind.INDEX_CONFLICT,
                    f.handler.replicate("n1",entry(1,99)).kind());
            assertEquals(entry(1,1),f.log.get(1).orElseThrow());
        }
    }
    @Test void failedCommitForceDoesNotApplyOrAcknowledgeAndClosesAdmission()throws Exception{
        try(var f=new Fixture()){
            f.put(1,1);f.wal.failBeforeCommit=true;
            IOException failure=assertThrows(IOException.class,()->f.handler.advanceCommit("n1",1));
            assertTrue(failure.getMessage().contains("force failed"));
            assertEquals(0,f.wal.durableCommitIndex());assertEquals(0,f.applier.appliedIndex());
            assertNull(f.gate.tryAcquire());assertNotNull(f.handler.storageFailure());
        }
    }

    @Test void postForceCommitIOExceptionLatchesFailureButWalRecoversCommit()throws Exception{
        try(var f=new Fixture()){
            f.put(1,1);f.wal.failAfterCommit=true;
            IOException problem=assertThrows(IOException.class,()->f.handler.advanceCommit("n1",1));
            assertTrue(problem.getMessage().contains("post-force"));
            assertSame(problem,f.handler.storageFailure());
            assertNotNull(f.failure.get());
            assertNull(f.gate.tryAcquire());
            assertThrows(FollowerReplicationHandler.StorageFailedException.class,
                    ()->f.handler.advanceCommit("n1",1));
        }
        try(var recovered=FileWalStore.open(directory.resolve("follower/raftkv.wal"))){
            assertEquals(1,recovered.durableCommitIndex());
        }
    }
    @Test void rewriteIOExceptionFailsClosedAndDoesNotAssumeRollback()throws Exception{
        try(var f=new Fixture()){
            f.put(1,1);f.put(2,2);f.handler.advanceCommit("n1",1);
            f.wal.failBeforeRewrite=true;
            assertThrows(IOException.class,()->f.handler.truncateUncommittedSuffix("n1",1));
            assertNotNull(f.handler.storageFailure());assertNull(f.gate.tryAcquire());
            assertEquals(2,f.log.lastLogIndex());
        }
    }
    @Test void restartReplayAppliesOnlyDurablyCommittedPrefix()throws Exception{
        try(var f=new Fixture()){
            f.put(1,1);f.put(2,2);f.handler.advanceCommit("n1",1);
        }
        try(var f=new Fixture()){
            assertEquals(2,f.log.lastLogIndex());assertEquals(1,f.applier.appliedIndex());
            assertTrue(f.applier.diagnosticGetForTest("key1").isPresent());
            assertTrue(f.applier.diagnosticGetForTest("key2").isEmpty());
        }
    }
    @Test void applicationFailureAfterForcedCommitDoesNotReturnSuccessOrReopenAdmission()throws Exception {
        try(var f=new Fixture()) {
            f.put(1,1);
            var latched=StateMachineApplier.class.getDeclaredField("failed");
            latched.setAccessible(true);latched.setBoolean(f.applier,true);
            assertThrows(IOException.class,()->f.handler.advanceCommit("n1",1));
            assertEquals(1,f.wal.durableCommitIndex());
            assertEquals(0,f.applier.appliedIndex());
            assertNotNull(f.handler.storageFailure());assertNull(f.gate.tryAcquire());
        }
    }
    @Test void forcedCommitIsNeverPublishedOrAppliedBeforeDurableCallReturns()throws Exception {
        var entered=new java.util.concurrent.CountDownLatch(1);
        var release=new java.util.concurrent.CountDownLatch(1);
        try(var f=new Fixture()) {
            f.put(1,1);
            // Override the single WAL's commit I/O through a controlled decorator.
            var blocking=new WalStore(){
                public WalRecoveryResult recoveryResult(){return f.wal.recoveryResult();}
                public void appendEntryDurably(LogEntry entry)throws IOException{f.wal.appendEntryDurably(entry);}
                public void appendCommitDurably(long n)throws IOException{
                    entered.countDown();
                    try {if(!release.await(3,java.util.concurrent.TimeUnit.SECONDS))throw new IOException("blocked force timeout");}
                    catch(InterruptedException e){Thread.currentThread().interrupt();throw new IOException(e);}
                    f.wal.appendCommitDurably(n);
                }
                public void rewriteDurably(List<LogEntry> entries,long n)throws IOException{f.wal.rewriteDurably(entries,n);}
                public long lastDurableLogIndex(){return f.wal.lastDurableLogIndex();}
                public long durableCommitIndex(){return f.wal.durableCommitIndex();}
                public void close()throws IOException{f.wal.close();}
            };
            var controlled=new FollowerReplicationHandler(ClusterConfigTest.config(directory,"n2"),f.log,blocking,
                    f.gate,f.status::get,n->{},f.applier,
                    n->f.publish(f.status.get().lastLogIndex(),n,f.status.get().appliedIndex()),
                    n->f.publish(f.status.get().lastLogIndex(),f.status.get().durableCommitIndex(),n),f.failure::set);
            var pool=java.util.concurrent.Executors.newSingleThreadExecutor();
            try {
                var call=pool.submit(()->controlled.advanceCommit("n1",1));
                assertTrue(entered.await(3,java.util.concurrent.TimeUnit.SECONDS));
                assertEquals(0,f.applier.appliedIndex());
                assertEquals(0,f.status.get().durableCommitIndex());
                release.countDown();assertEquals(1,call.get(3,java.util.concurrent.TimeUnit.SECONDS).appliedIndex());
            } finally {release.countDown();pool.shutdownNow();}
        }
    }

    @Test void closeAdmissionPreventsFurtherCommitAndTruncation()throws Exception{
        try(var f=new Fixture()){
            f.put(1,1);f.gate.closeAdmission();
            assertThrows(FollowerReplicationHandler.ProtocolRejectedException.class,
                    ()->f.handler.advanceCommit("n1",1));
            assertThrows(FollowerReplicationHandler.ProtocolRejectedException.class,
                    ()->f.handler.truncateUncommittedSuffix("n1",0));
        }
    }
}
