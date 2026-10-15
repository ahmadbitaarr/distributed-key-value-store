package io.github.ahmadbitaarr.raftkv.replication;

import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class ReplicationOperationGateTest {
    @Test void admissionCloseCannotRaceWithNewOperationPermit() throws Exception {
        ReplicationOperationGate gate = new ReplicationOperationGate();
        gate.open();
        ExecutorService pool = Executors.newFixedThreadPool(3);
        CountDownLatch admitted = new CountDownLatch(1);
        CountDownLatch releaseAdmitted = new CountDownLatch(1);
        CountDownLatch admissionClosed = new CountDownLatch(1);
        try {
            Future<?> first = pool.submit(() -> {
                try (var permit = gate.tryAcquire()) {
                    assertNotNull(permit, "operation begun before closure must be admitted");
                    admitted.countDown();
                    assertTrue(releaseAdmitted.await(5, TimeUnit.SECONDS));
                }
                return null;
            });
            assertTrue(admitted.await(5, TimeUnit.SECONDS));
            assertEquals(1, gate.activeCount(), "admitted operation must be counted");

            Future<?> closing = pool.submit(() -> {
                gate.closeAdmission();
                admissionClosed.countDown();
                return null;
            });
            Future<ReplicationOperationGate.Permit> afterClose = pool.submit(() -> {
                assertTrue(admissionClosed.await(5, TimeUnit.SECONDS));
                return gate.tryAcquire();
            });

            closing.get(5, TimeUnit.SECONDS);
            assertNull(afterClose.get(5, TimeUnit.SECONDS),
                    "an attempt ordered after admission closure must be rejected");
            assertNull(gate.tryAcquire(), "closure must remain in effect");
            assertEquals(1, gate.activeCount(),
                    "admission closure must not remove the earlier outstanding permit");
            assertFalse(gate.awaitDrained(0, TimeUnit.MILLISECONDS),
                    "a still-running admitted operation must prevent drainage");

            releaseAdmitted.countDown();
            first.get(5, TimeUnit.SECONDS);
            assertTrue(gate.awaitDrained(1, TimeUnit.SECONDS));
            assertEquals(0, gate.activeCount(), "permit release must decrement exactly once");
            assertNull(gate.tryAcquire(), "drainage must not reopen admission");

            // Force an acquisition attempt to contend with closure on the same monitor.
            // The contender cannot check open and increment between these atomic steps.
            ReplicationOperationGate contested = new ReplicationOperationGate();
            contested.open();
            CountDownLatch contenderStarted = new CountDownLatch(1);
            Future<ReplicationOperationGate.Permit> contender;
            synchronized (contested) {
                contender = pool.submit(() -> {
                    contenderStarted.countDown();
                    return contested.tryAcquire();
                });
                assertTrue(contenderStarted.await(5, TimeUnit.SECONDS));
                contested.closeAdmission();
                assertEquals(0, contested.activeCount());
            }
            assertNull(contender.get(5, TimeUnit.SECONDS),
                    "blocked acquisition must fail after the atomic close");
            assertEquals(0, contested.activeCount());
            assertTrue(contested.awaitDrained(1, TimeUnit.SECONDS));
        } finally {
            releaseAdmitted.countDown();
            pool.shutdownNow();
        }
    }

    @Test void admittedPermitRemainsUntilExplicitCompletion() throws Exception {
        ReplicationOperationGate gate = new ReplicationOperationGate();
        gate.open();
        var permit = gate.tryAcquire();
        assertNotNull(permit);
        gate.closeAdmission();
        assertNull(gate.tryAcquire());
        assertFalse(gate.awaitDrained(1, TimeUnit.MILLISECONDS));
        permit.close();
        permit.close();
        assertEquals(0, gate.activeCount());
        assertTrue(gate.awaitDrained(1, TimeUnit.SECONDS));
    }
}
