package io.github.ahmadbitaarr.raftkv.replication;

import io.github.ahmadbitaarr.raftkv.commit.AckTracker;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.server.NodeRole;
import io.github.ahmadbitaarr.raftkv.wal.FileWalStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.*;

/** The injected monotonic clock makes the dequeue/deadline boundary deterministic. */
class ReplicationManagerDeadlineTest {
    @TempDir Path dir;
    private static final long DEADLINE=TimeUnit.SECONDS.toNanos(5);
    private static LogEntry entry() {
        return new LogEntry(1,"deadline-request",Command.put("key",new byte[]{1}),new byte[]{7});
    }
    private static LongSupplier times(long... readings) {
        var calls=new AtomicInteger();
        return ()->readings[Math.min(calls.getAndIncrement(),readings.length-1)];
    }
    private static void waitOn(CountDownLatch latch) {
        try {
            if(!latch.await(5,TimeUnit.SECONDS)) throw new IllegalStateException("test latch timeout");
        } catch(InterruptedException e) {Thread.currentThread().interrupt();throw new IllegalStateException(e);}
    }

    @Test void completionObservedBeforeDeadlineCounts() throws Exception {
        try(var log=WalBackedPersistentLog.open(dir.resolve("wal"))) {
            log.append(entry());
            FollowerClient client=(id,e)->FollowerClient.ack(e.index(),e.index());
            try(var manager=new ReplicationManager(ClusterConfigTest.config(dir,"n1"),log,client,500,times(0,1,2,3))) {
                var answers=manager.replicateStoredEntryUntil(1,DEADLINE).outcomes();
                assertEquals(FollowerClient.Kind.ACK,answers.get("n2").kind());
                assertEquals(FollowerClient.Kind.ACK,answers.get("n3").kind());
            }
        }
    }

    @Test void completionObservedAfterDeadlineNeverCounts() throws Exception {
        try(var log=WalBackedPersistentLog.open(dir.resolve("wal"))) {
            log.append(entry());
            FollowerClient client=(id,e)->FollowerClient.ack(e.index(),e.index());
            try(var manager=new ReplicationManager(ClusterConfigTest.config(dir,"n1"),log,client,500,
                    times(0,DEADLINE+1))) {
                var answers=manager.replicateStoredEntryUntil(1,DEADLINE).outcomes();
                assertEquals(FollowerClient.Kind.UNKNOWN_TIMEOUT,answers.get("n2").kind());
                assertEquals(FollowerClient.Kind.UNKNOWN_TIMEOUT,answers.get("n3").kind());
            }
        }
    }

    @Test void ackExactlyAtDeadlineCannotBecomeMajorityCertificate() throws Exception {
        var config=ClusterConfigTest.config(dir,"n1");
        try(var wal=FileWalStore.open(config.walFile());var log=new WalBackedPersistentLog(wal)) {
            log.append(entry());
            var tracker=new AckTracker(config,log,wal);
            assertTrue(tracker.certifyEmpty("n2",new FollowerStatusProbe.Status("n2",NodeRole.FOLLOWER,0,0,0,false)));
            FollowerClient client=(id,e)->FollowerClient.ack(e.index(),e.index());
            try(var manager=new ReplicationManager(config,log,client,500,times(0,DEADLINE))) {
                var answers=manager.replicateStoredEntryUntil(1,DEADLINE).outcomes();
                assertEquals(FollowerClient.Kind.UNKNOWN_TIMEOUT,answers.get("n2").kind());
                assertFalse(tracker.recordDurableAck("n2",entry(),answers.get("n2")));
                assertTrue(tracker.majorityFor(entry()).isEmpty(),"late ACK must never provide commit proof");
                assertEquals(0,wal.durableCommitIndex());
            }
        }
    }

    @Test void oneTimelyFollowerAndOneLateOnlyTimelyProofCounts() throws Exception {
        var releaseLate=new CountDownLatch(1);
        var firstObserved=new CountDownLatch(1);
        var config=ClusterConfigTest.config(dir,"n1");
        try(var wal=FileWalStore.open(config.walFile());var log=new WalBackedPersistentLog(wal)) {
            log.append(entry());
            var tracker=new AckTracker(config,log,wal);
            for(String id:new String[]{"n2","n3"})
                assertTrue(tracker.certifyEmpty(id,new FollowerStatusProbe.Status(id,NodeRole.FOLLOWER,0,0,0,false)));
            FollowerClient client=(id,e)->{
                if(id.equals("n3"))waitOn(releaseLate);
                return FollowerClient.ack(e.index(),e.index());
            };
            AtomicInteger readings=new AtomicInteger();
            LongSupplier clock=()->{
                int n=readings.getAndIncrement();
                if(n==1)firstObserved.countDown(); // n2 completion was dequeued, pre-deadline
                return n<2?0:DEADLINE+1;
            };
            try(var manager=new ReplicationManager(config,log,client,500,clock)) {
                var answers=manager.replicateStoredEntryUntil(1,DEADLINE).outcomes();
                assertTrue(firstObserved.await(1,TimeUnit.SECONDS));
                assertEquals(FollowerClient.Kind.ACK,answers.get("n2").kind());
                assertEquals(FollowerClient.Kind.UNKNOWN_TIMEOUT,answers.get("n3").kind());
                assertTrue(tracker.recordDurableAck("n2",entry(),answers.get("n2")));
                assertFalse(tracker.recordDurableAck("n3",entry(),answers.get("n3")));
                assertEquals(1,tracker.verifiedMatchingThrough("n2"));
                assertEquals(0,tracker.verifiedMatchingThrough("n3"));
                releaseLate.countDown();
            } finally {releaseLate.countDown();}
        }
    }

    @Test void bothLateAcksCannotProvideMatchingPrefixProof() throws Exception {
        var config=ClusterConfigTest.config(dir,"n1");
        try(var wal=FileWalStore.open(config.walFile());var log=new WalBackedPersistentLog(wal)) {
            log.append(entry());
            var tracker=new AckTracker(config,log,wal);
            for(String id:new String[]{"n2","n3"})
                assertTrue(tracker.certifyEmpty(id,new FollowerStatusProbe.Status(id,NodeRole.FOLLOWER,0,0,0,false)));
            FollowerClient client=(id,e)->FollowerClient.ack(e.index(),e.index());
            try(var manager=new ReplicationManager(config,log,client,500,times(0,DEADLINE+1))) {
                var answers=manager.replicateStoredEntryUntil(1,DEADLINE).outcomes();
                for(String id:new String[]{"n2","n3"}) {
                    assertEquals(FollowerClient.Kind.UNKNOWN_TIMEOUT,answers.get(id).kind());
                    assertFalse(tracker.recordDurableAck(id,entry(),answers.get(id)));
                }
                assertTrue(tracker.majorityFor(entry()).isEmpty());
                assertEquals(0,wal.durableCommitIndex());
            }
        }
    }
}
