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
        InstrumentedWal(Path path)throws IOException {delegate=FileWalStore.open(path);}
        @Override public WalRecoveryResult recoveryResult(){return delegate.recoveryResult();}
        @Override public void appendEntryDurably(LogEntry entry)throws IOException {
            if(failEntry)throw new IOException("injected ENTRY force failure");delegate.appendEntryDurably(entry);
        }
        @Override public void appendCommitDurably(long n)throws IOException {
            commitAttempts.incrementAndGet();
            if(failCommit)throw new IOException("injected COMMIT force failure");
            delegate.appendCommitDurably(n); // the real FileWalStore has completed force(true)
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
        assertEquals(LeaderCoordinator.Code.NOT_READY,coordinator.mutate("duplicate",Command.put("b",new byte[]{2})).code());
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
}
