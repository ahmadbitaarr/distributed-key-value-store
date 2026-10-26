package io.github.ahmadbitaarr.raftkv.replication;

import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.log.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class ReplicationManagerCatchUpPriorityTest {
    @TempDir Path dir;
    private static LogEntry entry(){return new LogEntry(1,"id",Command.put("x",new byte[]{1}),new byte[]{1});}
    private static void await(CountDownLatch latch) {
        try {assertTrue(latch.await(3,TimeUnit.SECONDS));}
        catch(InterruptedException e){Thread.currentThread().interrupt();throw new AssertionError(e);}
    }
    @Test void liveReplicationUsesFreePeerWhileSlowBackgroundPeerIsBusy()throws Exception{
        try(var log=WalBackedPersistentLog.open(dir.resolve("wal"))){
            log.append(entry());
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
            FollowerClient transport=(id,e)->FollowerClient.ack(e.index(),e.index());
            try(var manager=new ReplicationManager(ClusterConfigTest.config(dir,"n1"),log,transport,200)){
                var worker=Executors.newSingleThreadExecutor();
                try {
                    var background=worker.submit(()->manager.backgroundAttempt("n2",()->{
                        entered.countDown();await(release);return "done";
                    }));
                    await(entered);
                    try(var live=manager.reserveLivePriority(100,id->id.equals("n3"))){
                        assertNotNull(live);
                        assertTrue(manager.backgroundAttempt("n3",()->"should not run").isEmpty());
                        var answers=manager.replicateStoredEntryUntil(1,
                                System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(150)).outcomes();
                        assertEquals(FollowerClient.Kind.ACK,answers.get("n3").kind());
                        assertEquals(FollowerClient.Kind.BUSY,answers.get("n2").kind());
                    }
                    release.countDown();assertEquals("done",background.get(2,TimeUnit.SECONDS).orElseThrow());
                    assertFalse(manager.isActive("n2"));
                }finally{release.countDown();worker.shutdownNow();}
            }
        }
    }
    @Test void cannotCreateUnresolvedSuffixOnlyBecauseOnlyCertifiedPeerIsBusy()throws Exception{
        try(var log=WalBackedPersistentLog.open(dir.resolve("wal"))){
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
            try(var manager=new ReplicationManager(ClusterConfigTest.config(dir,"n1"),log,
                    (id,e)->FollowerClient.ack(e.index(),e.index()),100)){
                var worker=Executors.newSingleThreadExecutor();
                try {
                    var blocked=worker.submit(()->manager.backgroundAttempt("n2",()->{
                        entered.countDown();await(release);return "finished";
                    }));
                    await(entered);
                    assertNull(manager.reserveLivePriority(20,id->id.equals("n2")),
                            "pre-append admission must defer rather than create a false NO_QUORUM");
                    assertEquals(0,log.lastLogIndex());
                    release.countDown();assertEquals("finished",blocked.get(2,TimeUnit.SECONDS).orElseThrow());
                    try(var live=manager.reserveLivePriority(100,id->id.equals("n2"))){
                        assertNotNull(live);
                    }
                }finally{release.countDown();worker.shutdownNow();}
            }
        }
    }
    @Test void liveWriteRetriesBackgroundBusyPeerAfterActualWorkerExit()throws Exception {
        try(var log=WalBackedPersistentLog.open(dir.resolve("wal"))){
            log.append(entry());
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
            var otherAttempted=new CountDownLatch(1);
            FollowerClient transport=(id,e)->{
                if(id.equals("n3")) {otherAttempted.countDown();
                    return new FollowerClient.Outcome(FollowerClient.Kind.UNAVAILABLE,e.index(),0,0,"offline");}
                return FollowerClient.ack(e.index(),e.index());
            };
            try(var manager=new ReplicationManager(ClusterConfigTest.config(dir,"n1"),log,transport,500)){
                var workers=Executors.newFixedThreadPool(2);
                try{
                    var background=workers.submit(()->manager.backgroundAttempt("n2",()->{
                        entered.countDown();await(release);return "finished";
                    }));
                    await(entered);
                    var decision=workers.submit(()->manager.replicateStoredEntryForLiveWrite(1,
                            System.nanoTime()+TimeUnit.SECONDS.toNanos(2),id->id.equals("n2")));
                    await(otherAttempted);
                    release.countDown();
                    var answers=decision.get(3,TimeUnit.SECONDS).outcomes();
                    assertEquals(FollowerClient.Kind.ACK,answers.get("n2").kind());
                    assertEquals(FollowerClient.Kind.UNAVAILABLE,answers.get("n3").kind());
                    assertEquals("finished",background.get(3,TimeUnit.SECONDS).orElseThrow());
                }finally{release.countDown();workers.shutdownNow();}
            }
        }
    }
    @Test void timelyCertifiedFollowerAckDoesNotWaitForSlowBackgroundPeer()throws Exception {
        try(var log=WalBackedPersistentLog.open(dir.resolve("wal"))){
            log.append(entry());
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
            try(var manager=new ReplicationManager(ClusterConfigTest.config(dir,"n1"),log,
                    (id,e)->FollowerClient.ack(e.index(),e.index()),500)){
                var workers=Executors.newSingleThreadExecutor();
                try {
                    var background=workers.submit(()->manager.backgroundAttempt("n2",()->{
                        entered.countDown();await(release);return 1;
                    }));
                    await(entered);
                    var answer=manager.replicateStoredEntryForLiveWrite(1,
                            System.nanoTime()+TimeUnit.SECONDS.toNanos(2),id->id.equals("n3")).outcomes();
                    assertEquals(FollowerClient.Kind.ACK,answer.get("n3").kind());
                    assertTrue(manager.isActive("n2"),"healthy ACK must not await unrelated catch-up worker");
                    assertEquals(FollowerClient.Kind.UNKNOWN_TIMEOUT,answer.get("n2").kind());
                    release.countDown();background.get(2,TimeUnit.SECONDS);
                }finally{release.countDown();workers.shutdownNow();}
            }
        }
    }

    @Test void managerCloseDoesNotMistakeCancelledBackgroundOperationForWorkerExit()throws Exception{
        try(var log=WalBackedPersistentLog.open(dir.resolve("wal"))){
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
            var manager=new ReplicationManager(ClusterConfigTest.config(dir,"n1"),log,
                    (id,e)->FollowerClient.ack(e.index(),e.index()),100);
            var worker=Executors.newSingleThreadExecutor();
            try {
                var blocked=worker.submit(()->manager.backgroundAttempt("n2",()->{
                    entered.countDown();await(release);return 1;
                }));
                await(entered);
                assertThrows(IOException.class,manager::close);
                assertTrue(manager.isActive("n2"));
                release.countDown();assertEquals(1,blocked.get(2,TimeUnit.SECONDS).orElseThrow());
                assertFalse(manager.isActive("n2"));
                manager.close();
            }finally{release.countDown();worker.shutdownNow();}
        }
    }
}
