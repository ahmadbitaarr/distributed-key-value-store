package io.github.ahmadbitaarr.raftkv.client;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.github.ahmadbitaarr.raftkv.config.NodeEndpoint;
import io.github.ahmadbitaarr.raftkv.log.Command;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class GrpcMutationTransportLifecycleTest {
    @Test void closeRacingBlockedChannelCreationCannotOrphanNewChannel() throws Exception {
        var constructing = new CountDownLatch(1);
        var finishConstruction = new CountDownLatch(1);
        var closing = new CountDownLatch(1);
        var created = new AtomicReference<ManagedChannel>();
        var factoryCalls = new AtomicInteger();
        var endpoint = new NodeEndpoint("127.0.0.1", 55011);
        var transport = new GrpcMutationTransport(ep -> {
            factoryCalls.incrementAndGet();
            constructing.countDown();
            try {
                if (!finishConstruction.await(5, TimeUnit.SECONDS))
                    throw new AssertionError("test did not release channel construction");
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new AssertionError(ex);
            }
            ManagedChannel channel = InProcessChannelBuilder.forName("close-race-" + UUID.randomUUID())
                    .directExecutor().build();
            created.set(channel);
            return channel;
        });
        try (var pool = Executors.newFixedThreadPool(2)) {
            try {
                var sending = pool.submit(() -> transport.send(endpoint, "r", Command.delete("key"), 1000));
                assertTrue(constructing.await(3, TimeUnit.SECONDS), "send did not enter channel creation");
                var closeTask = pool.submit(() -> {
                    closing.countDown();
                    transport.close();
                });
                assertTrue(closing.await(3, TimeUnit.SECONDS), "close was never requested");
                // The close cannot complete before the in-progress channel is published
                // and owned. The test blocks channel creation using latches, not sleeps.
                assertThrows(TimeoutException.class, () -> closeTask.get(100, TimeUnit.MILLISECONDS));
                finishConstruction.countDown();
                closeTask.get(4, TimeUnit.SECONDS);
                assertEquals(RetryingMutationClient.Code.UNAVAILABLE,
                        sending.get(4, TimeUnit.SECONDS).code());
                assertNotNull(created.get());
                assertTrue(created.get().isShutdown(), "close must shut down the newly built channel");
                assertEquals(1, factoryCalls.get());
                assertEquals(RetryingMutationClient.Code.UNAVAILABLE,
                        transport.send(endpoint, "again", Command.delete("key"), 100).code());
                assertEquals(1, factoryCalls.get(), "closed transport must not create new channels");
            } finally {
                finishConstruction.countDown();
                transport.close();
            }
        }
    }

    @Test void closeBeforeFirstSendPreventsAnyChannelCreation() {
        var created = new AtomicInteger();
        var endpoint = new NodeEndpoint("127.0.0.1", 55012);
        var transport = new GrpcMutationTransport(ep -> {
            created.incrementAndGet();
            throw new AssertionError("channel created after close");
        });
        transport.close();
        assertEquals(RetryingMutationClient.Code.UNAVAILABLE,
                transport.send(endpoint, "r", Command.delete("key"), 100).code());
        assertEquals(0, created.get());
        transport.close(); // idempotent
    }
}
