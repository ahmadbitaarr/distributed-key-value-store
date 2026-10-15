package io.github.ahmadbitaarr.raftkv.replication;

import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.config.NodeEndpoint;
import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class GrpcFollowerClientLifecycleRaceTest {
    @Test
    void concurrentCloseCannotLeaveUnownedChannel() throws Exception {
        CountDownLatch resolverEntered = new CountDownLatch(1);
        CountDownLatch releaseResolver = new CountDownLatch(1);
        AtomicInteger resolutions = new AtomicInteger();
        var config = ClusterConfigTest.config(
                java.nio.file.Files.createTempDirectory("client-race"), "n1");

        var client = new GrpcFollowerClient(config, id -> {
            resolutions.incrementAndGet();
            resolverEntered.countDown();
            try {
                if (!releaseResolver.await(5, TimeUnit.SECONDS))
                    throw new IllegalStateException("resolver timed out");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            return new NodeEndpoint("127.0.0.1", 1);
        });

        ExecutorService pool = Executors.newFixedThreadPool(2);
        LogEntry entry = new LogEntry(1, "race",
                Command.put("key", new byte[]{1}), new byte[]{2});
        try {
            Future<?> replication = pool.submit(() -> client.replicate("n2", entry));
            assertTrue(resolverEntered.await(5, TimeUnit.SECONDS));

            CountDownLatch closeStarted = new CountDownLatch(1);
            java.util.concurrent.atomic.AtomicReference<Thread> closeThread = new java.util.concurrent.atomic.AtomicReference<>();
            Future<?> closing = pool.submit(() -> {
                closeThread.set(Thread.currentThread());
                closeStarted.countDown();
                try {
                    client.close();
                } catch (java.io.IOException e) {
                    throw new RuntimeException(e);
                }
            });

            assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (closeThread.get().getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
                Thread.yield();
            }
            assertEquals(Thread.State.BLOCKED, closeThread.get().getState(),
                    "shutdown must wait for channel registration lock");
            assertFalse(closing.isDone());
            releaseResolver.countDown();
            replication.get(10, TimeUnit.SECONDS);
            closing.get(10, TimeUnit.SECONDS);

            var field = GrpcFollowerClient.class.getDeclaredField("channels");
            field.setAccessible(true);
            Map<?, ?> channels = (Map<?, ?>) field.get(client);
            assertTrue(channels.isEmpty(), "closed client retained channels");

            assertEquals(FollowerClient.Kind.REMOTE_ERROR,
                    client.replicate("n2", entry).kind());
            assertEquals(1, resolutions.get(),
                    "replication after closure resolved a new endpoint");
        } finally {
            releaseResolver.countDown();
            pool.shutdownNow();
            client.close();
        }
    }
}
