package io.github.ahmadbitaarr.raftkv.replication;

import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class ReplicationManagerOwnershipTest {
    @TempDir Path dir;
    private static LogEntry entry(){return new LogEntry(1, "request-1", Command.put("key", new byte[]{0, -1, 13}), new byte[]{1,2,3});}

    @Test void timeoutDoesNotReleaseFollowerSlotWhileWorkerRuns() throws Exception {
        ownershipScenario(false);
    }
    @Test void secondAttemptReturnsBusyUntilFirstWorkerExits() throws Exception {
        ownershipScenario(false);
    }
    @Test void followerSlotReleasedAfterActualWorkerCompletion() throws Exception {
        ownershipScenario(true);
    }
    private void ownershipScenario(boolean waitForCompletion) throws Exception {
        CountDownLatch entered = new CountDownLatch(1), exit = new CountDownLatch(1);
        FollowerClient blocking = (id, e) -> {
            entered.countDown();
            try { exit.await(); } catch (InterruptedException ignored) { // simulate a blocking non-cancellable RPC
                try { exit.await(); } catch (InterruptedException second) { Thread.currentThread().interrupt(); }
            }
            return FollowerClient.ack(e.index(), e.index());
        };
        try (WalBackedPersistentLog log=WalBackedPersistentLog.open(dir.resolve("leader.wal"))) {
            log.append(entry());
            ReplicationManager manager=new ReplicationManager(ClusterConfigTest.config(dir.resolve("configured"),"n1"),log,blocking,30);
            try {
                var first=manager.replicateStoredEntry(1,"n2");
                assertEquals(FollowerClient.Kind.UNKNOWN_TIMEOUT, first.kind());
                assertTrue(entered.await(2,TimeUnit.SECONDS));
                assertEquals(FollowerClient.Kind.BUSY,manager.replicateStoredEntry(1,"n2").kind());
                assertEquals(0,manager.highestAcceptedRequestIndex("n2"));
                exit.countDown();
                long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
                while(manager.isActive("n2") && System.nanoTime()<until) Thread.onSpinWait();
                assertFalse(manager.isActive("n2"));
                if(waitForCompletion) assertEquals(FollowerClient.Kind.ACK,manager.replicateStoredEntry(1,"n2").kind());
                manager.close();
            } finally { exit.countDown(); }
        }
    }

    @Test void managerCloseReportsUnterminatedWorker() throws Exception {
        CountDownLatch entered=new CountDownLatch(1), exit=new CountDownLatch(1);
        FollowerClient blocking=(id,e)->{
            entered.countDown();
            boolean finished=false;
            while(!finished){ try{exit.await();finished=true;}catch(InterruptedException ignored){} }
            return FollowerClient.ack(e.index(),e.index());
        };
        try(WalBackedPersistentLog log=WalBackedPersistentLog.open(dir.resolve("leader.wal"))){
            log.append(entry());
            ReplicationManager manager=new ReplicationManager(ClusterConfigTest.config(dir.resolve("configured"),"n1"),log,blocking,20);
            try {
                assertEquals(FollowerClient.Kind.UNKNOWN_TIMEOUT,manager.replicateStoredEntry(1,"n2").kind());
                assertTrue(entered.await(1,TimeUnit.SECONDS));
                assertThrows(IOException.class,manager::close);
                assertTrue(manager.isActive("n2"));
            } finally {
                exit.countDown();
                long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
                while(manager.isActive("n2") && System.nanoTime()<until) Thread.onSpinWait();
                manager.close();
            }
        }
    }
    @Test void parallelFanoutDispatchesToBothFollowersBeforeWaiting()throws Exception {
        CountDownLatch bothEntered=new CountDownLatch(2),release=new CountDownLatch(1);
        FollowerClient client=(id,e)->{
            bothEntered.countDown();
            try{release.await(3,TimeUnit.SECONDS);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();}
            return FollowerClient.ack(e.index(),e.index());
        };
        try(WalBackedPersistentLog log=WalBackedPersistentLog.open(dir.resolve("leader.wal"))){
            log.append(entry());
            ReplicationManager manager=new ReplicationManager(ClusterConfigTest.config(dir.resolve("configured"),"n1"),log,client,200);
            try {
                ExecutorService driver=Executors.newSingleThreadExecutor();
                try {
                    Future<ReplicationManager.BatchResult> batch=driver.submit(()->manager.replicateStoredEntry(1));
                    assertTrue(bothEntered.await(1,TimeUnit.SECONDS),"slow n2 must not delay sending to n3");
                    release.countDown();
                    var answers=batch.get(2,TimeUnit.SECONDS).outcomes();
                    assertEquals(FollowerClient.Kind.ACK,answers.get("n2").kind());
                    assertEquals(FollowerClient.Kind.ACK,answers.get("n3").kind());
                    assertEquals(1,manager.highestAcceptedRequestIndex("n2"));
                    assertEquals(1,manager.highestAcceptedRequestIndex("n3"));
                }finally{release.countDown();driver.shutdownNow();}
            }finally{release.countDown();manager.close();}
        }
    }
    @Test void rejectionNeverAdvancesDurableAckProgress()throws Exception {
        FollowerClient rejecting=(id,e)->new FollowerClient.Outcome(FollowerClient.Kind.INDEX_GAP,e.index(),0,1,"gap");
        try(WalBackedPersistentLog log=WalBackedPersistentLog.open(dir.resolve("leader.wal"))){
            log.append(entry());
            try(ReplicationManager manager=new ReplicationManager(ClusterConfigTest.config(dir.resolve("configured"),"n1"),log,rejecting,200)){
                var response=manager.replicateStoredEntry(1,"n2");
                assertEquals(FollowerClient.Kind.INDEX_GAP,response.kind());
                assertEquals(0,manager.highestAcceptedRequestIndex("n2"));
            }
        }
    }
    @Test void invalidAckCoverageNeverAdvancesProgress()throws Exception {
        FollowerClient wrong=(id,e)->new FollowerClient.Outcome(FollowerClient.Kind.ACK,e.index(),0,0,"invalid");
        try(WalBackedPersistentLog log=WalBackedPersistentLog.open(dir.resolve("leader.wal"))){
            log.append(entry());
            try(ReplicationManager manager=new ReplicationManager(ClusterConfigTest.config(dir.resolve("configured"),"n1"),log,wrong,200)){
                assertEquals(FollowerClient.Kind.INVALID_RESPONSE,manager.replicateStoredEntry(1,"n2").kind());
                assertEquals(0,manager.highestAcceptedRequestIndex("n2"));
            }
        }
    }

}
