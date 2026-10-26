package io.github.ahmadbitaarr.raftkv.leader;

import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.server.*;
import io.github.ahmadbitaarr.raftkv.statemachine.*;
import org.junit.jupiter.api.Test;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;
import static org.junit.jupiter.api.Assertions.*;

class LeaderReadCoordinatorTest {
    private static NodeStatusSnapshot status(long last,long commit,long applied) {
        return new NodeStatusSnapshot("n1",NodeRole.LEADER,NodeLifecycleState.READY,last,commit,applied,true);
    }
    private static LogEntry entry(long index,Command command) {
        return new LogEntry(index,"r"+index,command,new byte[]{1});
    }
    private final StateMachineApplier applier=new StateMachineApplier(new KvStateMachine());
    private final AtomicReference<NodeStatusSnapshot> status=new AtomicReference<>(status(0,0,0));
    private final LeaderReadCoordinator reads=new LeaderReadCoordinator(applier,status::get);

    @Test void freshLeaderNotFoundAtCommitZero() {
        reads.openAfterStartup();
        var r=reads.read("missing");
        assertEquals(LeaderReadCoordinator.Code.NOT_FOUND,r.code());
        assertEquals(0,r.commitIndex());assertTrue(r.value().isEmpty());
    }
    @Test void readBeforeAdmissionAndAfterClosureReturnsNotReady() {
        assertEquals(LeaderReadCoordinator.Code.NOT_READY,reads.read("x").code());
        reads.openAfterStartup();reads.closeAdmission();
        assertEquals(LeaderReadCoordinator.Code.NOT_READY,reads.read("x").code());
        assertThrows(IllegalStateException.class,reads::openAfterStartup);
    }
    @Test void committedPutAndDeleteAreReadInOrder() {
        var log=new InMemoryPersistentLog();
        reads.openAfterStartup();
        log.append(entry(1,Command.put("x",new byte[]{0,2,0,3})));
        applier.applyThrough(1,1,log);status.set(status(1,1,1));
        var first=reads.read("x");
        assertEquals(LeaderReadCoordinator.Code.OK,first.code());
        assertArrayEquals(new byte[]{0,2,0,3},first.value().orElseThrow());
        assertEquals(1,first.commitIndex());
        log.append(entry(2,Command.put("x",new byte[0])));
        applier.applyThrough(2,2,log);status.set(status(2,2,2));
        assertEquals(0,reads.read("x").value().orElseThrow().length);
        log.append(entry(3,Command.delete("x")));
        applier.applyThrough(3,3,log);status.set(status(3,3,3));
        assertEquals(LeaderReadCoordinator.Code.NOT_FOUND,reads.read("x").code());
        assertEquals(3,reads.read("x").commitIndex());
        assertEquals(3,log.lastLogIndex(),"GET must not write to the log");
    }
    @Test void defensiveResultsCannotChangeStoredBytes() {
        var log=new InMemoryPersistentLog();log.append(entry(1,Command.put("x",new byte[]{7,8})));
        applier.applyThrough(1,1,log);status.set(status(1,1,1));reads.openAfterStartup();
        var result=reads.read("x");result.value().orElseThrow()[0]=55;
        assertArrayEquals(new byte[]{7,8},result.value().orElseThrow());
        assertArrayEquals(new byte[]{7,8},reads.read("x").value().orElseThrow());
    }
    @Test void noUncommittedOrPartiallyPublishedStateExposed() {
        reads.openAfterStartup();
        var log=new InMemoryPersistentLog();log.append(entry(1,Command.put("x",new byte[]{1})));
        status.set(status(1,0,0)); // locally durable ENTRY, not COMMIT
        assertEquals(LeaderReadCoordinator.Code.NOT_FOUND,reads.read("x").code());
        status.set(status(1,1,0)); // COMMIT forced but application outstanding
        assertEquals(LeaderReadCoordinator.Code.NOT_READY,reads.read("x").code());
        applier.applyThrough(1,1,log); // applied but snapshot not published yet
        assertEquals(LeaderReadCoordinator.Code.NOT_READY,reads.read("x").code());
        status.set(status(1,1,1));
        assertArrayEquals(new byte[]{1},reads.read("x").value().orElseThrow());
        log.append(entry(2,Command.put("x",new byte[]{2})));
        status.set(status(2,1,1)); // unresolved write suffix blocks writes, not reads
        assertArrayEquals(new byte[]{1},reads.read("x").value().orElseThrow());
        assertEquals(1,reads.read("x").commitIndex());
    }
    @Test void staleOrFailedSnapshotNeverReturnsValue() {
        var log=new InMemoryPersistentLog();log.append(entry(1,Command.put("x",new byte[]{1})));
        applier.applyThrough(1,1,log);reads.openAfterStartup();
        status.set(status(1,1,0));assertEquals(LeaderReadCoordinator.Code.NOT_READY,reads.read("x").code());
        status.set(new NodeStatusSnapshot("n1",NodeRole.LEADER,NodeLifecycleState.FAILED,1,1,1,false));
        assertEquals(LeaderReadCoordinator.Code.NOT_READY,reads.read("x").code());
        status.set(new NodeStatusSnapshot("n1",NodeRole.LEADER,NodeLifecycleState.STOPPING,1,1,1,false));
        assertEquals(LeaderReadCoordinator.Code.NOT_READY,reads.read("x").code());
    }
    @Test void readPermitAcquisitionAndClosureDrainWithoutCheckThenIncrementRace() throws Exception {
        CountDownLatch entered=new CountDownLatch(1),release=new CountDownLatch(1);
        var stalled=new LeaderReadCoordinator(applier,()->{
            entered.countDown();
            try {if(!release.await(5,TimeUnit.SECONDS))throw new IllegalStateException("timed out waiting for test");}
            catch(InterruptedException e){Thread.currentThread().interrupt();throw new RuntimeException(e);}
            return status.get();
        });
        stalled.openAfterStartup();
        ExecutorService pool=Executors.newSingleThreadExecutor();
        try {
            Future<LeaderReadCoordinator.ReadResult> active=pool.submit(()->stalled.read("x"));
            assertTrue(entered.await(5,TimeUnit.SECONDS));
            assertEquals(1,stalled.inFlightReads());
            stalled.closeAdmission();
            assertEquals(LeaderReadCoordinator.Code.NOT_READY,stalled.read("x").code());
            assertEquals(1,stalled.inFlightReads());
            assertFalse(stalled.awaitDrained(0,TimeUnit.NANOSECONDS));
            release.countDown();
            assertEquals(LeaderReadCoordinator.Code.NOT_FOUND,active.get(5,TimeUnit.SECONDS).code());
            assertTrue(stalled.awaitDrained(5,TimeUnit.SECONDS));
            assertEquals(0,stalled.inFlightReads());
        }finally{release.countDown();pool.shutdownNow();assertTrue(pool.awaitTermination(5,TimeUnit.SECONDS));}
    }
    @Test void multipleConcurrentReadPermitsAllowedAndDrainCorrectly() throws Exception {
        CountDownLatch entered=new CountDownLatch(2),release=new CountDownLatch(1);
        // The state-machine monitor is intentionally serial; admission permits can still overlap.
        var stalled=new LeaderReadCoordinator(applier,()->{
            entered.countDown();
            try {release.await(10,TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();throw new RuntimeException(e);}
            return status.get();
        });
        stalled.openAfterStartup();
        ExecutorService pool=Executors.newFixedThreadPool(2);
        try {
            var a=pool.submit(()->stalled.read("a"));
            var b=pool.submit(()->stalled.read("b"));
            // One read is inside state access, the other has acquired a permit while waiting for it.
            // Wait using an eventual count bounded by the test deadline, not a sleep as the correctness oracle.
            long stop=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
            while(stalled.inFlightReads()!=2 && System.nanoTime()<stop)Thread.onSpinWait();
            assertEquals(2,stalled.inFlightReads());
            stalled.closeAdmission();
            release.countDown();
            assertEquals(LeaderReadCoordinator.Code.NOT_FOUND,a.get(5,TimeUnit.SECONDS).code());
            assertEquals(LeaderReadCoordinator.Code.NOT_FOUND,b.get(5,TimeUnit.SECONDS).code());
            assertEquals(0,stalled.inFlightReads());
        }finally{release.countDown();pool.shutdownNow();assertTrue(pool.awaitTermination(5,TimeUnit.SECONDS));}
    }
}
