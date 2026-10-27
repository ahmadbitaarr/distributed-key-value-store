package io.github.ahmadbitaarr.raftkv.recovery;

import io.github.ahmadbitaarr.raftkv.leader.CanonicalCommandFingerprint;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.statemachine.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class RecoveryManagerLocalTest {
    @TempDir Path root;
    private static LogEntry entry(long index,String id,Command command){
        return new LogEntry(index,id,command,CanonicalCommandFingerprint.of(command));
    }
    private Path path(){return root.resolve("raftkv.wal");}
    private static class DelegatingStore implements WalStore {
        final WalStore delegate;
        DelegatingStore(WalStore delegate){this.delegate=delegate;}
        public WalRecoveryResult recoveryResult(){return delegate.recoveryResult();}
        public void appendEntryDurably(LogEntry e)throws IOException{delegate.appendEntryDurably(e);}
        public void appendCommitDurably(long i)throws IOException{delegate.appendCommitDurably(i);}
        public void rewriteDurably(List<LogEntry> entries,long commit)throws IOException{delegate.rewriteDurably(entries,commit);}
        public long lastDurableLogIndex(){return delegate.lastDurableLogIndex();}
        public long durableCommitIndex(){return delegate.durableCommitIndex();}
        public void close()throws IOException{delegate.close();}
    }
    @Test void committedBinaryEmptyOverwriteDeleteReplayAndTailDiscardSurviveReopen()throws Exception {
        try(var wal=FileWalStore.open(path())){
            wal.appendEntryDurably(entry(1,"id-1",Command.put("x",new byte[]{1,0,3})));
            wal.appendEntryDurably(entry(2,"id-2",Command.put("x",new byte[0])));
            wal.appendEntryDurably(entry(3,"id-3",Command.put("gone",new byte[]{8})));
            wal.appendEntryDurably(entry(4,"id-4",Command.delete("gone")));
            wal.appendCommitDurably(4);
            wal.appendEntryDurably(entry(5,"uncommitted",Command.put("x",new byte[]{99})));
        }
        try(var wal=FileWalStore.open(path())) {
            var log=new WalBackedPersistentLog(wal);
            var applier=new StateMachineApplier(new KvStateMachine());
            assertEquals(new RecoveryManager.LocalState(4,4),RecoveryManager.recoverLocal(log,wal,applier));
            assertArrayEquals(new byte[0],applier.diagnosticGetForTest("x").orElseThrow());
            assertTrue(applier.diagnosticGetForTest("gone").isEmpty());
            assertEquals(4,log.lastLogIndex());
            assertEquals(4,wal.durableCommitIndex());
            assertTrue(log.get(5).isEmpty());
        }
        try(var reopened=FileWalStore.open(path())){
            assertEquals(4,reopened.lastDurableLogIndex());
            assertEquals(4,reopened.durableCommitIndex());
            assertEquals("id-2",reopened.recoveryResult().entries().get(1).requestId());
            assertArrayEquals(CanonicalCommandFingerprint.of(Command.put("x",new byte[0])),
                    reopened.recoveryResult().entries().get(1).commandFingerprint());
        }
    }
    @Test void entirelyUncommittedSuffixCanBeCanonicalizedToZeroAndIndexReusedSafely()throws Exception {
        try(var wal=FileWalStore.open(path())) {
            wal.appendEntryDurably(entry(1,"abandoned",Command.put("x",new byte[]{7})));
        }
        try(var wal=FileWalStore.open(path())) {
            var log=new WalBackedPersistentLog(wal);var applier=new StateMachineApplier(new KvStateMachine());
            assertEquals(0,RecoveryManager.recoverLocal(log,wal,applier).committedIndex());
            assertEquals(0,log.lastLogIndex());
            assertTrue(applier.diagnosticGetForTest("x").isEmpty());
            log.append(entry(1,"new-request",Command.put("x",new byte[]{8})));
            assertEquals("new-request",log.get(1).orElseThrow().requestId());
        }
        try(var reopened=FileWalStore.open(path())){
            assertEquals("new-request",reopened.recoveryResult().entries().get(0).requestId());
            assertEquals(0,reopened.durableCommitIndex());
        }
    }
    @Test void cleanCommittedWalNeverRequestsAnUnnecessaryRewrite()throws Exception {
        try(var wal=FileWalStore.open(path())) {
            wal.appendEntryDurably(entry(1,"1",Command.put("x",new byte[]{1})));
            wal.appendCommitDurably(1);
        }
        try(var store=new DelegatingStore(FileWalStore.open(path())){
            @Override public void rewriteDurably(List<LogEntry> e,long c)throws IOException {
                fail("clean WAL must not be rewritten");
            }
        }) {
            var applier=new StateMachineApplier(new KvStateMachine());
            assertEquals(1,RecoveryManager.recoverLocal(new WalBackedPersistentLog(store),store,applier).appliedIndex());
            assertArrayEquals(new byte[]{1},applier.diagnosticGetForTest("x").orElseThrow());
        }
    }
    @Test void rewriteFailureBeforeReplacementNeverAuthorizesReplay()throws Exception {
        try(var wal=FileWalStore.open(path())){
            wal.appendEntryDurably(entry(1,"1",Command.put("x",new byte[]{1})));
            wal.appendEntryDurably(entry(2,"2",Command.put("x",new byte[]{2})));
            wal.appendCommitDurably(1);
        }
        try(var store=new DelegatingStore(FileWalStore.open(path())){
            @Override public void rewriteDurably(List<LogEntry> e,long c)throws IOException {
                throw new IOException("pre-rewrite failure");
            }
        }) {
            assertTrue(assertThrows(IOException.class,()->RecoveryManager.recoverLocal(
                    new WalBackedPersistentLog(store),store,new StateMachineApplier(new KvStateMachine())))
                    .getMessage().contains("pre-rewrite"));
        }
        try(var wal=FileWalStore.open(path())){
            assertEquals(2,wal.lastDurableLogIndex());assertEquals(1,wal.durableCommitIndex());
        }
    }
    @Test void postRewriteUncertainFailureStillFailsClosedEvenIfReplacementIsDurable()throws Exception {
        try(var wal=FileWalStore.open(path())){
            wal.appendEntryDurably(entry(1,"1",Command.put("x",new byte[]{1})));
            wal.appendCommitDurably(1);
            wal.appendEntryDurably(entry(2,"uncommitted",Command.put("x",new byte[]{2})));
        }
        try(var store=new DelegatingStore(FileWalStore.open(path())){
            @Override public void rewriteDurably(List<LogEntry> e,long c)throws IOException {
                super.rewriteDurably(e,c);throw new IOException("post-force outcome unknown");
            }
        }) {
            var applier=new StateMachineApplier(new KvStateMachine());
            IOException failure=assertThrows(IOException.class,()->RecoveryManager.recoverLocal(
                    new WalBackedPersistentLog(store),store,applier));
            assertEquals("post-force outcome unknown",failure.getMessage());
            assertEquals(0,applier.appliedIndex());
        }
        try(var reopened=FileWalStore.open(path())){
            assertEquals(1,reopened.lastDurableLogIndex());
            assertEquals(1,reopened.durableCommitIndex());
        }
    }
    @Test void repeatedIndependentLeaderRecoveryProducesExactlySameAppliedState()throws Exception {
        try(var wal=FileWalStore.open(path())){
            wal.appendEntryDurably(entry(1,"x1",Command.put("x",new byte[]{1})));
            wal.appendEntryDurably(entry(2,"x2",Command.put("x",new byte[]{9,0})));
            wal.appendCommitDurably(2);
        }
        for(int pass=0;pass<3;pass++) {
            try(var wal=FileWalStore.open(path())) {
                var applier=new StateMachineApplier(new KvStateMachine());
                var result=RecoveryManager.recoverLocal(new WalBackedPersistentLog(wal),wal,applier);
                assertEquals(new RecoveryManager.LocalState(2,2),result);
                assertArrayEquals(new byte[]{9,0},applier.diagnosticGetForTest("x").orElseThrow());
            }
        }
    }
}
