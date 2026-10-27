package io.github.ahmadbitaarr.raftkv.leader;

import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class RequestDeduplicatorTest {
    @TempDir Path dir;
    private static byte[] fp(Command c){return CanonicalCommandFingerprint.of(c);}
    private static LogEntry entry(long i,String id,Command c){return new LogEntry(i,id,c,fp(c));}
    private static RequestDeduplicator.Completion ok(long index){return new RequestDeduplicator.Completion(RequestDeduplicator.CompletionCode.OK,index);}

    @Test void ownerJoinsAndPublishesOneCommittedRecordAtomically(){
        var d=new RequestDeduplicator();var c=Command.put("x",new byte[]{1});
        var first=d.classify("id",c,fp(c));assertEquals(RequestDeduplicator.Kind.NEW,first.kind());
        var joined=d.classify("id",c,fp(c));assertEquals(RequestDeduplicator.Kind.JOIN,joined.kind());
        assertSame(joined.completion(),d.classify("id",c,fp(c)).completion());
        assertEquals(ok(7),d.finish(first.owner(),ok(7),false));
        assertEquals(ok(7),joined.completion().join());
        var committed=d.classify("id",c,fp(c));
        assertEquals(RequestDeduplicator.Kind.COMMITTED_DUPLICATE,committed.kind());
        assertEquals(7,committed.commitIndex());assertEquals(0,d.activeCount());assertEquals(1,d.committedCount());
    }
    @Test void equalFingerprintWithDifferentCommandNeverHidesConflict(){
        var d=new RequestDeduplicator();var a=Command.put("x",new byte[]{1});var b=Command.put("x",new byte[]{2});
        byte[] same=fp(a);var owner=d.classify("id",a,same).owner();
        assertEquals(RequestDeduplicator.Kind.CONFLICT,d.classify("id",b,same).kind());
        d.finish(owner,ok(1),false);
        assertEquals(RequestDeduplicator.Kind.CONFLICT,d.classify("id",b,same).kind());
        assertEquals(RequestDeduplicator.Kind.CONFLICT,d.classify("id",a,fp(b)).kind());
    }
    @Test void putDeleteAndDistinctKeysConflictWithinAndAfterFlight(){
        var d=new RequestDeduplicator();var original=Command.put("x",new byte[0]);
        var owned=d.classify("same",original,fp(original)).owner();
        for(var other:List.of(Command.delete("x"),Command.put("y",new byte[0]),Command.put("x",new byte[]{0})))
            assertEquals(RequestDeduplicator.Kind.CONFLICT,d.classify("same",other,fp(other)).kind());
        d.finish(owned,ok(1),false);
        assertEquals(RequestDeduplicator.Kind.CONFLICT,d.classify("same",Command.delete("x"),fp(Command.delete("x"))).kind());
    }
    @Test void sourceBytesAndFingerprintCannotBeMutatedAfterRegistration(){
        byte[] original={0,4};var c=Command.put("k",original);byte[] digest=fp(c);
        var d=new RequestDeduplicator();var owner=d.classify("id",c,digest).owner();
        original[0]=7;digest[0]^=3;
        assertEquals(RequestDeduplicator.Kind.JOIN,d.classify("id",Command.put("k",new byte[]{0,4}),fp(c)).kind());
        d.finish(owner,ok(1),false);
        assertEquals(RequestDeduplicator.Kind.COMMITTED_DUPLICATE,d.classify("id",c,fp(c)).kind());
    }
    @Test void incompletePreappendOwnerReleasesReservationAndMayRetry(){
        var d=new RequestDeduplicator();var c=Command.delete("x");
        var first=d.classify("i",c,fp(c));
        d.finish(first.owner(),new RequestDeduplicator.Completion(RequestDeduplicator.CompletionCode.NOT_READY,0),false);
        assertEquals(RequestDeduplicator.Kind.NEW,d.classify("i",c,fp(c)).kind());
    }
    @Test void unresolvedOwnerNeverAllowsAnotherIndexForSameRequest(){
        var d=new RequestDeduplicator();var c=Command.delete("x");var first=d.classify("i",c,fp(c));
        d.finish(first.owner(),new RequestDeduplicator.Completion(RequestDeduplicator.CompletionCode.NO_QUORUM,0),true);
        assertEquals(RequestDeduplicator.Kind.UNRESOLVED,d.classify("i",c,fp(c)).kind());
        assertEquals(RequestDeduplicator.Kind.CONFLICT,d.classify("i",Command.delete("y"),fp(Command.delete("y"))).kind());
    }
    @Test void timeoutOfJoinedWaiterDoesNotCancelOriginal(){
        var d=new RequestDeduplicator();var c=Command.delete("x");var first=d.classify("id",c,fp(c));
        var join=d.classify("id",c,fp(c));
        assertEquals(RequestDeduplicator.CompletionCode.NOT_READY,RequestDeduplicator.await(join.completion(),1).code());
        assertFalse(join.completion().isDone());
        d.finish(first.owner(),ok(9),false);
        assertEquals(ok(9),join.completion().join());
    }
    @Test void shutdownWakesJoinersButDoesNotInventCommittedSuccess(){
        var d=new RequestDeduplicator();var c=Command.delete("x");var first=d.classify("id",c,fp(c));
        var join=d.classify("id",c,fp(c));d.closeAdmission();
        assertEquals(RequestDeduplicator.CompletionCode.NOT_READY,join.completion().join().code());
        assertEquals(RequestDeduplicator.Kind.CLOSED,d.classify("id",c,fp(c)).kind());
        assertEquals(RequestDeduplicator.CompletionCode.NOT_READY,d.finish(first.owner(),ok(1),false).code());
        assertEquals(0,d.committedCount());
    }
    @Test void concurrentOwnerCompletionNeverAllowsSecondNewRegistration()throws Exception {
        var d=new RequestDeduplicator();var c=Command.put("x",new byte[]{8});
        var first=d.classify("race",c,fp(c));var pool=Executors.newFixedThreadPool(8);
        var go=new CountDownLatch(1);
        try {
            List<Future<RequestDeduplicator.Kind>> calls=new ArrayList<>();
            for(int i=0;i<48;i++)calls.add(pool.submit(()->{go.await();return d.classify("race",c,fp(c)).kind();}));
            go.countDown();d.finish(first.owner(),ok(1),false);
            for(var call:calls)assertNotEquals(RequestDeduplicator.Kind.NEW,call.get(5,TimeUnit.SECONDS));
            assertEquals(1,d.committedCount());
        }finally{pool.shutdownNow();}
    }
    @Test void fileBackedCommittedHistoryRestoresExactRequestIdAndIndex()throws Exception {
        var path=dir.resolve("wal");
        try(var w=FileWalStore.open(path)){
            var a=Command.put("x",new byte[]{0,1});var b=Command.delete("y");
            w.appendEntryDurably(entry(1,"put",a));w.appendEntryDurably(entry(2,"del",b));w.appendCommitDurably(2);
        }
        try(var w=FileWalStore.open(path);var log=new WalBackedPersistentLog(w)){
            var d=new RequestDeduplicator();d.reconstruct(log,2);
            assertEquals(2,d.committedCount());assertEquals(1,d.classify("put",Command.put("x",new byte[]{0,1}),fp(Command.put("x",new byte[]{0,1}))).commitIndex());
            assertEquals(2,d.classify("del",Command.delete("y"),fp(Command.delete("y"))).commitIndex());
        }
    }
    @Test void incorrectCommittedFingerprintCausesFailClosedReconstruction()throws Exception {
        try(var w=FileWalStore.open(dir.resolve("bad"));var log=new WalBackedPersistentLog(w)){
            var c=Command.put("x",new byte[]{4});log.append(new LogEntry(1,"id",c,new byte[]{0}));w.appendCommitDurably(1);
            assertThrows(IOException.class,()->new RequestDeduplicator().reconstruct(log,1));
        }
    }
    @Test void duplicateCommittedIdentifiersAtDifferentIndexesFailClosed()throws Exception {
        try(var w=FileWalStore.open(dir.resolve("duplicate"));var log=new WalBackedPersistentLog(w)){
            log.append(entry(1,"id",Command.put("x",new byte[]{1})));
            log.append(entry(2,"id",Command.delete("x")));
            w.appendCommitDurably(2);
            assertThrows(IOException.class,()->new RequestDeduplicator().reconstruct(log,2));
        }
    }
    @Test void uncommittedSuffixCannotBeReconstructedAsCommitted()throws Exception {
        try(var w=FileWalStore.open(dir.resolve("tail"));var log=new WalBackedPersistentLog(w)){
            log.append(entry(1,"ok",Command.delete("x")));w.appendCommitDurably(1);
            log.append(entry(2,"abandoned",Command.delete("y")));
            var d=new RequestDeduplicator();d.reconstruct(log,1);
            assertEquals(RequestDeduplicator.Kind.NEW,d.classify("abandoned",Command.delete("y"),fp(Command.delete("y"))).kind());
            assertEquals(1,d.committedCount());
        }
    }
}
