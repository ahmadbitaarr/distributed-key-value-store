package io.github.ahmadbitaarr.raftkv.leader;

import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class LeaderMutationGateTest {
    @Test void onlyOneLeaderWriteCanAcquire(){var gate=new LeaderMutationGate();gate.open();var first=gate.tryAcquire();assertNotNull(first);assertNull(gate.tryAcquire());first.close();assertNotNull(gate.tryAcquire());gate.closeAdmission();}
    @Test void shutdownClosesAdmissionAndWaitsForActualOperation()throws Exception {
        var gate=new LeaderMutationGate();gate.open();var first=gate.tryAcquire();gate.closeAdmission();
        assertNull(gate.tryAcquire());assertFalse(gate.awaitDrained(1,TimeUnit.MILLISECONDS));
        first.close();assertTrue(gate.awaitDrained(1,TimeUnit.SECONDS));
    }
    @Test void raceAdmissionAndShutdownNeverGrantsAfterClosure()throws Exception {
        var gate=new LeaderMutationGate();gate.open();var first=gate.tryAcquire();
        var pool=Executors.newFixedThreadPool(2);var barrier=new CountDownLatch(1);
        try {
            var close=pool.submit(()->{gate.closeAdmission();barrier.countDown();});
            assertTrue(barrier.await(2,TimeUnit.SECONDS));close.get();
            var attempted=pool.submit(gate::tryAcquire);assertNull(attempted.get());
            assertTrue(gate.isActive());first.close();assertTrue(gate.awaitDrained(2,TimeUnit.SECONDS));
        }finally{pool.shutdownNow();}
    }
}
