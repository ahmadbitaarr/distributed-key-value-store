package io.github.ahmadbitaarr.raftkv.leader;

import io.github.ahmadbitaarr.raftkv.commit.*;
import io.github.ahmadbitaarr.raftkv.config.*;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.replication.*;
import io.github.ahmadbitaarr.raftkv.server.NodeRole;
import io.github.ahmadbitaarr.raftkv.statemachine.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class LeaderCoordinatorTest {
    @TempDir Path dir;
    private static final class InstrumentedWal implements WalStore {
        final FileWalStore delegate;
        boolean failEntry,failCommit,failCommitAfterForce;
        final AtomicInteger commitAttempts=new AtomicInteger();
        volatile CountDownLatch entryForceEntered, releaseEntryForce;
        volatile Runnable afterCommitForced=()->{};
        InstrumentedWal(Path path)throws IOException {delegate=FileWalStore.open(path);}
        @Override public WalRecoveryResult recoveryResult(){return delegate.recoveryResult();}
        @Override public void appendEntryDurably(LogEntry entry)throws IOException {
            if(entryForceEntered!=null){
                entryForceEntered.countDown();
                try {if(!releaseEntryForce.await(8,TimeUnit.SECONDS))throw new IOException("force barrier expired");}
                catch(InterruptedException x){Thread.currentThread().interrupt();throw new IOException("force interrupted",x);}
            }
            if(failEntry)throw new IOException("injected ENTRY force failure");delegate.appendEntryDurably(entry);
        }
        @Override public void appendCommitDurably(long n)throws IOException {
            commitAttempts.incrementAndGet();
            if(failCommit)throw new IOException("injected COMMIT force failure");
            delegate.appendCommitDurably(n); // the real FileWalStore has completed force(true)
            afterCommitForced.run();
            if(failCommitAfterForce)throw new IOException("injected error after durable COMMIT force");
        }
        @Override public void rewriteDurably(List<LogEntry> retained,long n)throws IOException {delegate.rewriteDurably(retained,n);}
        @Override public long lastDurableLogIndex(){return delegate.lastDurableLogIndex();}
        @Override public long durableCommitIndex(){return delegate.durableCommitIndex();}
        @Override public void close()throws IOException{delegate.close();}
    }
    private InstrumentedWal wal;
    private WalBackedPersistentLog log;
    private ReplicationManager manager;
    private StateMachineApplier applier;
    private LeaderCoordinator coordinator;
    private final AtomicLong publishedLog=new AtomicLong(), publishedCommit=new AtomicLong(), publishedApplied=new AtomicLong();
    private final AtomicReference<IOException> failure=new AtomicReference<>();
    private AtomicInteger outbound;
    private void start(FollowerClient client,FollowerStatusProbe probe)throws Exception {
        var cfg=ClusterConfigTest.config(dir,"n1");
        wal=new InstrumentedWal(cfg.walFile());log=new WalBackedPersistentLog(wal);
        manager=new ReplicationManager(cfg,log,client,500);
        var tracker=new AckTracker(cfg,log,wal);
        var committer=new CommitManager(tracker,wal,log,publishedCommit::set);
        applier=new StateMachineApplier(new KvStateMachine());
        coordinator=new LeaderCoordinator(cfg,log,wal,manager,probe,tracker,committer,applier,
                publishedLog::set,publishedApplied::set,failure::set);
        coordinator.openAfterStartup();
    }
    private static FollowerStatusProbe emptyFollowers(){
        return id->Optional.of(new FollowerStatusProbe.Status(id,NodeRole.FOLLOWER,0,0,0,false));
    }
    private static FollowerClient ackN2Only(){return (id,e)->id.equals("n2")?
            FollowerClient.ack(e.index(),e.index()):new FollowerClient.Outcome(FollowerClient.Kind.UNAVAILABLE,e.index(),0,0,"down");}
    private static FollowerClient unavailable(){return (id,e)->new FollowerClient.Outcome(FollowerClient.Kind.UNAVAILABLE,e.index(),0,0,"down");}
    @AfterEach void close()throws Exception {
        if(manager!=null)manager.close();
        if(log!=null)log.close();
    }
    @Test void healthyLeaderAndOneCertifiedFollowerCommitAndApply()throws Exception {
        start(ackN2Only(),emptyFollowers());var result=coordinator.mutate("r1",Command.put("x",new byte[]{1}));
        assertEquals(LeaderCoordinator.Code.OK,result.code());assertEquals(1,result.commitIndex());
        assertEquals(1,publishedLog.get());assertEquals(1,publishedCommit.get());assertEquals(1,publishedApplied.get());
        assertArrayEquals(new byte[]{1},applier.diagnosticGetForTest("x").orElseThrow());
    }
    @Test void consecutiveWritesCommitAndApplyInOrder()throws Exception {
        start(ackN2Only(),emptyFollowers());assertEquals(1,coordinator.mutate("r1",Command.put("x",new byte[0])).commitIndex());
        assertEquals(2,coordinator.mutate("r2",Command.delete("x")).commitIndex());
        assertEquals(2,wal.durableCommitIndex());assertEquals(2,applier.appliedIndex());
        assertTrue(applier.diagnosticGetForTest("x").isEmpty());
    }
    @Test void backgroundContentionBeforeAppendReturnsNotReadyAndDoesNotLatchUnresolvedSuffix() throws Exception {
        start(ackN2Only(), id -> id.equals("n2")
                ? Optional.of(new FollowerStatusProbe.Status(id,NodeRole.FOLLOWER,0,0,0,false))
                : Optional.empty());
        var occupied=new CountDownLatch(1);
        var release=new CountDownLatch(1);
        var pool=Executors.newSingleThreadExecutor();
        try {
            var background=pool.submit(()->manager.backgroundAttempt("n2",()->{
                occupied.countDown();
                while(release.getCount()>0) {
                    try {release.await();}catch(InterruptedException ignored) { /* simulate uncancellable RPC */ }
                }
                return "completed";
            }));
            assertTrue(occupied.await(3,TimeUnit.SECONDS));
            var blocked=coordinator.mutate("r1",Command.put("x",new byte[]{1}));
            assertEquals(LeaderCoordinator.Code.NOT_READY,blocked.code());
            assertEquals(0,wal.lastDurableLogIndex(),"do not append before securing a usable follower slot");
            assertEquals(0,publishedLog.get());
            assertEquals(0,wal.durableCommitIndex());
            assertFalse(coordinator.hasUnresolvedSuffix(),"local contention is not NO_QUORUM");
            assertEquals(0,applier.appliedIndex());
            release.countDown();
            assertEquals("completed",background.get(3,TimeUnit.SECONDS).orElseThrow());
            var recovered=coordinator.mutate("r1",Command.put("x",new byte[]{1}));
            assertEquals(LeaderCoordinator.Code.OK,recovered.code());
            assertEquals(1,wal.lastDurableLogIndex());
            assertEquals(1,wal.durableCommitIndex());
            assertEquals(1,applier.appliedIndex());
            assertFalse(coordinator.hasUnresolvedSuffix());
        } finally {release.countDown();pool.shutdownNow();}
    }
    @Test void noQuorumLatchesUnresolvedSuffixAndPreventsNextAppend()throws Exception {
        start(unavailable(),emptyFollowers());
        assertEquals(LeaderCoordinator.Code.NO_QUORUM,coordinator.mutate("r1",Command.put("x",new byte[]{1})).code());
        assertTrue(coordinator.hasUnresolvedSuffix());assertEquals(1,wal.lastDurableLogIndex());
        assertEquals(LeaderCoordinator.Code.NOT_READY,coordinator.mutate("r2",Command.put("y",new byte[]{2})).code());
        assertEquals(LeaderCoordinator.Code.NOT_READY,coordinator.mutate("r1",Command.put("x",new byte[]{1})).code());
        assertEquals(0,wal.durableCommitIndex());assertEquals(0,applier.appliedIndex());
    }
    @Test void reportedHighWaterMarkCannotReplaceExactPrefixProof()throws Exception {
        start((id,e)->FollowerClient.ack(e.index(),999),id->Optional.empty());
        assertEquals(LeaderCoordinator.Code.NO_QUORUM,coordinator.mutate("r1",Command.put("x",new byte[]{1})).code());
        assertEquals(0,wal.durableCommitIndex());
    }
    @Test void committedRequestIdNeverAppendsAgain()throws Exception {
        start(ackN2Only(),emptyFollowers());
        assertEquals(LeaderCoordinator.Code.OK,coordinator.mutate("duplicate",Command.put("a",new byte[]{1})).code());
        assertEquals(LeaderCoordinator.Code.REQUEST_ID_CONFLICT,coordinator.mutate("duplicate",Command.put("b",new byte[]{2})).code());
        assertEquals(1,log.lastLogIndex());
    }
    @Test void localEntryForceFailurePreventsReplicationAndCommit()throws Exception {
        outbound=new AtomicInteger();
        start((id,e)->{outbound.incrementAndGet();return FollowerClient.ack(e.index(),e.index());},emptyFollowers());
        wal.failEntry=true;
        assertThrows(IOException.class,()->coordinator.mutate("failure",Command.put("x",new byte[]{1})));
        assertEquals(0,outbound.get());assertEquals(0,publishedLog.get());assertEquals(0,wal.durableCommitIndex());
        assertNotNull(failure.get());assertEquals(LeaderCoordinator.Code.NOT_READY,
                coordinator.mutate("later",Command.put("x",new byte[]{1})).code());
    }
    @Test void commitForceFailurePreventsSuccessAndFailsClosed()throws Exception {
        start(ackN2Only(),emptyFollowers());wal.failCommit=true;
        assertThrows(IOException.class,()->coordinator.mutate("r1",Command.put("x",new byte[]{1})));
        assertEquals(1,publishedLog.get());assertEquals(0,publishedCommit.get());
        assertEquals(0,publishedApplied.get());assertNotNull(failure.get());
        assertEquals(LeaderCoordinator.Code.NOT_READY,coordinator.mutate("r2",Command.put("y",new byte[]{2})).code());
    }
    @Test void failureAfterSuccessfulCommitForceFailsClosedAndRecoveredCommitRemainsDurable() throws Exception {
        start(ackN2Only(),emptyFollowers());
        wal.failCommitAfterForce=true;
        IOException original=assertThrows(IOException.class,
                ()->coordinator.mutate("uncertain-commit",Command.put("x",new byte[]{9})));
        assertEquals("injected error after durable COMMIT force",original.getMessage());
        assertSame(original,failure.get(),"diagnostics must retain the original post-force error");
        assertSame(original,coordinator.storageFailure());
        assertTrue(coordinator.hasUnresolvedSuffix());
        assertEquals(1,wal.lastDurableLogIndex());
        assertEquals(1,wal.durableCommitIndex(),"COMMIT was forced before error was injected");
        assertEquals(0,publishedCommit.get(),"failed call cannot publish successful commit");
        assertEquals(0,publishedApplied.get(),"failed call cannot apply or respond OK");
        assertEquals(1,wal.commitAttempts.get());
        assertEquals(LeaderCoordinator.Code.NOT_READY,
                coordinator.mutate("another",Command.delete("x")).code());
        assertEquals(1,wal.commitAttempts.get(),"never attempt another speculative COMMIT");
        assertEquals(1,log.lastLogIndex(),"never append behind the uncertain outcome");

        var config=ClusterConfigTest.config(dir,"n1");
        manager.close();manager=null;
        log.close();log=null;
        try(var recovered=FileWalStore.open(config.walFile())) {
            assertEquals(1,recovered.lastDurableLogIndex());
            assertEquals(1,recovered.durableCommitIndex(),"clean recovery discovers the forced COMMIT");
            var recoveredLog=new WalBackedPersistentLog(recovered);
            try(var recoveredManager=new ReplicationManager(config,recoveredLog,unavailable(),500)) {
                var tracker=new AckTracker(config,recoveredLog,recovered);
                var committer=new CommitManager(tracker,recovered,recoveredLog,ignored->{});
                var restarted=new LeaderCoordinator(config,recoveredLog,recovered,recoveredManager,
                        emptyFollowers(),tracker,committer,new StateMachineApplier(new KvStateMachine()),
                        ignored->{},ignored->{},ignored->{});
                restarted.openAfterStartup();
                assertEquals(LeaderCoordinator.Code.NOT_READY,
                        restarted.mutate("restart-write",Command.put("x",new byte[]{2})).code());
                assertEquals(1,recovered.durableCommitIndex());
            }
        }
    }

    @Test void bothFollowerAcksAfterNoQuorumCannotCommitOrApply() throws Exception {
        var entered=new CountDownLatch(2);
        var release=new CountDownLatch(1);
        start((id,e)->{
            entered.countDown();
            boolean finished=false;
            while(!finished) {
                try {release.await();finished=true;}
                catch(InterruptedException ignored) { /* intentionally uncancellable remote call */ }
            }
            return FollowerClient.ack(e.index(),e.index());
        },emptyFollowers());
        var thread=Executors.newSingleThreadExecutor();
        try {
            var pending=thread.submit(()->coordinator.mutate("late-both",Command.put("x",new byte[]{1})));
            assertTrue(entered.await(3,TimeUnit.SECONDS));
            assertEquals(LeaderCoordinator.Code.NO_QUORUM,pending.get(8,TimeUnit.SECONDS).code());
            assertTrue(coordinator.hasUnresolvedSuffix());
            release.countDown();
            long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
            while((manager.isActive("n2")||manager.isActive("n3")) && System.nanoTime()<until)
                Thread.onSpinWait();
            assertFalse(manager.isActive("n2"));
            assertFalse(manager.isActive("n3"));
            assertEquals(1,manager.highestAcceptedRequestIndex("n2"));
            assertEquals(1,manager.highestAcceptedRequestIndex("n3"));
            assertEquals(0,wal.durableCommitIndex());
            assertEquals(0,publishedCommit.get());
            assertEquals(0,applier.appliedIndex());
            assertEquals(LeaderCoordinator.Code.NOT_READY,
                    coordinator.mutate("next",Command.delete("x")).code());
            assertEquals(1,log.lastLogIndex());
        } finally {release.countDown();thread.shutdownNow();}
    }

    @Test void atomicSingleFlightDoesNotAssignSameIndex()throws Exception {
        var entered=new CountDownLatch(1);var exit=new CountDownLatch(1);
        start((id,e)->{if(id.equals("n2")){entered.countDown();try{if(!exit.await(5,TimeUnit.SECONDS))throw new IllegalStateException("timed out");}catch(InterruptedException ex){Thread.currentThread().interrupt();}}return FollowerClient.ack(e.index(),e.index());},emptyFollowers());
        var worker=Executors.newSingleThreadExecutor();
        try {
            Future<LeaderCoordinator.MutationResult> pending=worker.submit(()->coordinator.mutate("first",Command.put("x",new byte[]{1})));
            assertTrue(entered.await(3,TimeUnit.SECONDS));
            assertEquals(LeaderCoordinator.Code.NOT_READY,coordinator.mutate("second",Command.put("y",new byte[]{2})).code());
            exit.countDown();assertEquals(LeaderCoordinator.Code.OK,pending.get(5,TimeUnit.SECONDS).code());
            assertEquals(1,log.lastLogIndex());
        }finally{exit.countDown();worker.shutdownNow();}
    }
    @Test void shutdownAdmissionWaitsOnLeaderWorker()throws Exception {
        var entered=new CountDownLatch(1);var exit=new CountDownLatch(1);
        start((id,e)->{if(id.equals("n2")){entered.countDown();try{exit.await(5,TimeUnit.SECONDS);}catch(InterruptedException x){Thread.currentThread().interrupt();}}return unavailable().replicate(id,e);},emptyFollowers());
        var executor=Executors.newSingleThreadExecutor();
        try {
            var pending=executor.submit(()->coordinator.mutate("r1",Command.delete("x")));
            assertTrue(entered.await(3,TimeUnit.SECONDS));
            coordinator.closeAdmission();
            assertFalse(coordinator.awaitDrained(5,TimeUnit.MILLISECONDS));
            assertEquals(LeaderCoordinator.Code.NOT_READY,coordinator.mutate("next",Command.delete("y")).code());
            exit.countDown();pending.get(5,TimeUnit.SECONDS);
            assertTrue(coordinator.awaitDrained(1,TimeUnit.SECONDS));
        }finally{exit.countDown();executor.shutdownNow();}
    }
    @Test void lateFollowerAckAfterNoQuorumNeverRetroactivelyCommits()throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        start((id,e)->{
            if(id.equals("n2")) {
                entered.countDown();
                while(true) {try {release.await();break;}catch(InterruptedException ignored){ /* simulated uncancellable I/O */ }}
                return FollowerClient.ack(e.index(),e.index());
            }
            return new FollowerClient.Outcome(FollowerClient.Kind.UNAVAILABLE,e.index(),0,0,"offline");
        },emptyFollowers());
        var thread=Executors.newSingleThreadExecutor();
        try {
            var pending=thread.submit(()->coordinator.mutate("timed-out",Command.put("x",new byte[]{1})));
            assertTrue(entered.await(3,TimeUnit.SECONDS));
            assertEquals(LeaderCoordinator.Code.NO_QUORUM,pending.get(8,TimeUnit.SECONDS).code());
            release.countDown();
            long limit=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
            while(manager.isActive("n2") && System.nanoTime()<limit)Thread.onSpinWait();
            assertFalse(manager.isActive("n2"));
            assertEquals(1,manager.highestAcceptedRequestIndex("n2")); // diagnostic only
            assertEquals(0,wal.durableCommitIndex());
            assertEquals(LeaderCoordinator.Code.NOT_READY,coordinator.mutate("later",Command.delete("y")).code());
            assertEquals(1,log.lastLogIndex());
        }finally{release.countDown();thread.shutdownNow();}
    }

    @Test void exactCommittedPutAndDeleteRetryReturnOriginalIndexWithoutAppending()throws Exception {
        start(ackN2Only(),emptyFollowers());
        var a=Command.put("x",new byte[]{0,9});var b=Command.delete("x");
        assertEquals(1,coordinator.mutate("id-put",a).commitIndex());
        assertEquals(1,coordinator.mutate("id-put",a).commitIndex());
        assertEquals(2,coordinator.mutate("id-delete",b).commitIndex());
        assertEquals(2,coordinator.mutate("id-delete",b).commitIndex());
        assertEquals(2,wal.durableCommitIndex());assertEquals(2,log.lastLogIndex());
        assertEquals(2,applier.appliedIndex());
    }
    @Test void sameIdDifferentOperationKeyOrValueIsConflictWithoutNewEntry()throws Exception {
        start(ackN2Only(),emptyFollowers());
        assertEquals(LeaderCoordinator.Code.OK,coordinator.mutate("same",Command.put("x",new byte[0])).code());
        assertEquals(LeaderCoordinator.Code.REQUEST_ID_CONFLICT,coordinator.mutate("same",Command.delete("x")).code());
        assertEquals(LeaderCoordinator.Code.REQUEST_ID_CONFLICT,coordinator.mutate("same",Command.put("y",new byte[0])).code());
        assertEquals(LeaderCoordinator.Code.REQUEST_ID_CONFLICT,coordinator.mutate("same",Command.put("x",new byte[]{0})).code());
        assertEquals(1,log.lastLogIndex());
    }
    @Test void concurrentIdenticalMutationJoinsOwnerDuringBlockedFollowerRpc()throws Exception {
        var entered=new CountDownLatch(1);var exit=new CountDownLatch(1);
        start((id,e)->{
            if(id.equals("n2")) {entered.countDown();try {exit.await(5,TimeUnit.SECONDS);}catch(InterruptedException x){Thread.currentThread().interrupt();}}
            return id.equals("n2")?FollowerClient.ack(e.index(),e.index()):unavailable().replicate(id,e);
        },emptyFollowers());
        var executor=Executors.newFixedThreadPool(3);
        try {
            var c=Command.put("x",new byte[]{1,0});
            var original=executor.submit(()->coordinator.mutate("join-id",c));
            assertTrue(entered.await(3,TimeUnit.SECONDS));
            var waiter=executor.submit(()->coordinator.mutate("join-id",c));
            assertEquals(LeaderCoordinator.Code.REQUEST_ID_CONFLICT,coordinator.mutate("join-id",Command.delete("x")).code());
            assertEquals(LeaderCoordinator.Code.NOT_READY,coordinator.mutate("unrelated",Command.delete("y")).code());
            exit.countDown();
            assertEquals(1,original.get(8,TimeUnit.SECONDS).commitIndex());
            assertEquals(1,waiter.get(8,TimeUnit.SECONDS).commitIndex());
            assertEquals(1,log.lastLogIndex());assertEquals(1,wal.durableCommitIndex());
        } finally {exit.countDown();executor.shutdownNow();}
    }
    @Test void joiningTimeoutDoesNotEndOriginalMutationOrCreateAnotherEntry()throws Exception {
        start(ackN2Only(),emptyFollowers());
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        wal.entryForceEntered=entered;wal.releaseEntryForce=release;
        var executor=Executors.newFixedThreadPool(2);
        try {
            var original=executor.submit(()->coordinator.mutate("stable",Command.delete("x")));
            assertTrue(entered.await(3,TimeUnit.SECONDS));
            assertEquals(LeaderCoordinator.Code.REQUEST_ID_CONFLICT,
                    coordinator.mutate("stable",Command.delete("y")).code(),
                    "conflict during blocked ENTRY force must reject without a second owner");
            var waiter=executor.submit(()->coordinator.mutate("stable",Command.delete("x")));
            assertEquals(LeaderCoordinator.Code.NOT_READY,waiter.get(5,TimeUnit.SECONDS).code());
            assertEquals(0,wal.lastDurableLogIndex(),"joined timeout must not force ENTRY itself");
            release.countDown();
            assertEquals(LeaderCoordinator.Code.NO_QUORUM,original.get(5,TimeUnit.SECONDS).code()); // ENTRY force consumed its fixed Phase 7 deadline
            assertEquals(1,log.lastLogIndex());
        }finally{release.countDown();executor.shutdownNow();}
    }
    @Test void failedEntryForceCannotBeCachedAsCommittedDuplicate()throws Exception {
        start(ackN2Only(),emptyFollowers());wal.failEntry=true;
        assertThrows(IOException.class,()->coordinator.mutate("fault-id",Command.put("x",new byte[]{1})));
        assertEquals(LeaderCoordinator.Code.NOT_READY,coordinator.mutate("fault-id",Command.put("x",new byte[]{1})).code());
        assertEquals(0,wal.durableCommitIndex());
    }

    @Test void committedDuplicateIssuesZeroNewReplicationAndWALCommands()throws Exception {
        var calls=new AtomicInteger();
        start((id,e)->{calls.incrementAndGet();return ackN2Only().replicate(id,e);},emptyFollowers());
        var c=Command.put("x",new byte[]{3});
        assertEquals(1,coordinator.mutate("original",c).commitIndex());
        int before=calls.get();
        assertEquals(1,coordinator.mutate("original",c).commitIndex());
        assertEquals(before,calls.get(),"duplicate may not contact followers");
        assertEquals(1,wal.lastDurableLogIndex());assertEquals(1,wal.durableCommitIndex());
        assertEquals(1,applier.appliedIndex());
    }
    @Test void multipleConcurrentIdenticalJoinersShareOneCommitAndSingleWalEntry()throws Exception {
        start(ackN2Only(),emptyFollowers());
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        wal.entryForceEntered=entered;wal.releaseEntryForce=release;
        var executor=Executors.newFixedThreadPool(6);
        try {
            var c=Command.put("k",new byte[]{3});
            var owner=executor.submit(()->coordinator.mutate("once",c));
            assertTrue(entered.await(3,TimeUnit.SECONDS));
            var joiners=new ArrayList<Future<LeaderCoordinator.MutationResult>>();
            for(int i=0;i<5;i++)joiners.add(executor.submit(()->coordinator.mutate("once",c)));
            release.countDown();
            assertEquals(1,owner.get(8,TimeUnit.SECONDS).commitIndex());
            for(var joined:joiners)assertEquals(1,joined.get(8,TimeUnit.SECONDS).commitIndex());
            assertEquals(1,wal.lastDurableLogIndex());assertEquals(1,wal.durableCommitIndex());
        }finally{release.countDown();executor.shutdownNow();}
    }
    @Test void fatalCommitForceFailureWakesIdenticalWaiterAndNeverCachesOk()throws Exception {
        start(ackN2Only(),emptyFollowers());
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        wal.entryForceEntered=entered;wal.releaseEntryForce=release;wal.failCommitAfterForce=true;
        var executor=Executors.newFixedThreadPool(2);
        try {
            var c=Command.put("k",new byte[]{3});
            var owner=executor.submit(()->coordinator.mutate("broken",c));
            assertTrue(entered.await(3,TimeUnit.SECONDS));
            var joiner=executor.submit(()->coordinator.mutate("broken",c));
            release.countDown();
            assertThrows(ExecutionException.class,()->owner.get(8,TimeUnit.SECONDS));
            assertEquals(LeaderCoordinator.Code.NOT_READY,joiner.get(8,TimeUnit.SECONDS).code());
            assertNotNull(coordinator.storageFailure());
            assertEquals(1,wal.durableCommitIndex(),"the COMMIT force did physically finish");
            assertEquals(1,wal.lastDurableLogIndex(),"no second ENTRY was ever appended");
        }finally{release.countDown();executor.shutdownNow();}
    }

    @Test void realStateMachineApplicationFailureAfterCommitDoesNotCacheDuplicateOk()throws Exception {
        start(ackN2Only(),emptyFollowers());
        var poisoned=Command.delete("x");
        wal.afterCommitForced=()->{
            try {
                var field=Command.class.getDeclaredField("type");field.setAccessible(true);
                field.set(poisoned,OperationType.PUT); // real KvStateMachine.apply rejects missing PUT value
            }catch(ReflectiveOperationException e){throw new AssertionError(e);}
        };
        assertThrows(IOException.class,()->coordinator.mutate("application-fault",poisoned));
        assertEquals(1,wal.durableCommitIndex(),"COMMIT force physically succeeded");
        assertEquals(0,applier.appliedIndex(),"failed command cannot claim applied progress");
        assertNotNull(coordinator.storageFailure());
        assertEquals(0,coordinator.deduplicatorForTest().committedCount(),
                "no successful duplicate record after application failure");
        assertEquals(LeaderCoordinator.Code.NOT_READY,
                coordinator.mutate("application-fault",Command.delete("x")).code());
        assertEquals(1,wal.lastDurableLogIndex(),"must never append a second entry");
    }
    @Test void authorityFailureInCommitWakeupWindowCannotCompleteDuplicateAsOk()throws Exception {
        start(ackN2Only(),emptyFollowers());
        coordinator.setCommitWakeup(()->coordinator.failExternally(new IOException("injected post-commit authority loss")));
        var result=coordinator.mutate("authority-race",Command.put("x",new byte[]{1}));
        assertEquals(LeaderCoordinator.Code.NOT_READY,result.code(),"closed admission suppresses racy OK publication");
        assertEquals(1,wal.durableCommitIndex(),"COMMIT remains durable, not rolled back");
        assertEquals(1,applier.appliedIndex());
        assertEquals(0,coordinator.deduplicatorForTest().committedCount());
        assertNotNull(coordinator.storageFailure());
        assertEquals(LeaderCoordinator.Code.NOT_READY,
                coordinator.mutate("authority-race",Command.put("x",new byte[]{1})).code());
        assertEquals(1,wal.lastDurableLogIndex());
    }
}
