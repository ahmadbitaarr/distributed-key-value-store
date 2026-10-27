package io.github.ahmadbitaarr.raftkv.server;

import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.wal.FileWalStore;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.io.IOException;
import static org.junit.jupiter.api.Assertions.*;

class NodeServerTest {
 @TempDir Path dir;
 private NodeServer node(String id,Path walDir) {return new NodeServer(ClusterConfigTest.config(walDir,id),0);}
 private void wal(Path directory, boolean committed)throws Exception{Files.createDirectories(directory);try(var store=FileWalStore.open(directory.resolve("raftkv.wal"))){store.appendEntryDurably(new LogEntry(1,"rid",Command.put("x",new byte[]{42}),io.github.ahmadbitaarr.raftkv.leader.CanonicalCommandFingerprint.of(Command.put("x",new byte[]{42}))));if(committed)store.appendCommitDurably(1);}}
 @Test void newEmptyWalStartsReadyOnlyOnLeader()throws Exception{
  try(var n=node("n1",dir)){n.start();assertEquals(NodeLifecycleState.READY,n.statusSnapshot().state());assertTrue(n.statusSnapshot().ready());}
  try(var n=node("n2",dir.resolve("follower"))){n.start();assertEquals(NodeLifecycleState.CONTROL_ONLY,n.statusSnapshot().state());assertFalse(n.statusSnapshot().ready());}
 }
 @Test void previouslyOpenedEmptyWalRequiresRecoveryWitness() throws Exception {
  // Physical emptiness is not proof that the WAL never held an uncommitted ENTRY.
  Path path=dir.resolve("raftkv.wal");
  try(var wal=FileWalStore.open(path)) {
   assertEquals(0,wal.lastDurableLogIndex());
   assertEquals(0,wal.durableCommitIndex());
  }
  assertTrue(Files.exists(path));
  try(var n=node("n1",dir)) {
   n.start();
   var status=n.statusSnapshot();
   assertEquals(NodeLifecycleState.CONTROL_ONLY,status.state());
   assertFalse(status.ready(),"an existing empty WAL still requires a verified follower witness");
   assertEquals(0,status.lastLogIndex());
   assertEquals(0,status.durableCommitIndex());
   assertEquals(0,status.appliedIndex());
  }
 }
 @Test void committedWalStartsInControlOnlyNotReady()throws Exception{wal(dir,true);try(var n=node("n1",dir)){n.start();assertEquals(1,n.statusSnapshot().durableCommitIndex());assertEquals(1,n.statusSnapshot().appliedIndex());assertEquals(NodeLifecycleState.CONTROL_ONLY,n.statusSnapshot().state());assertFalse(n.statusSnapshot().ready());}}
 @Test void uncommittedWalStartsInControlOnlyNotReady()throws Exception{wal(dir,false);try(var n=node("n1",dir)){n.start();assertEquals(0,n.statusSnapshot().lastLogIndex());assertEquals(0,n.statusSnapshot().durableCommitIndex());assertEquals(0,n.statusSnapshot().appliedIndex());assertEquals(NodeLifecycleState.CONTROL_ONLY,n.statusSnapshot().state());assertFalse(n.statusSnapshot().ready());}}
 @Test void startupRecoversWalBeforeBinding()throws Exception{wal(dir,true);try(var n=node("n1",dir)){assertEquals(NodeLifecycleState.NEW,n.statusSnapshot().state());n.start();assertEquals(1,n.statusSnapshot().lastLogIndex());assertTrue(n.boundPort()>0);}}
 @Test void startupRecoversActualDurableStatusIndexes()throws Exception{wal(dir,true);try(var n=node("n2",dir)){n.start();assertEquals(1,n.statusSnapshot().lastLogIndex());assertEquals(1,n.statusSnapshot().durableCommitIndex());}}
 @Test void recoveredLeaderReplaysCommittedCommandsWithoutPrematureReadiness()throws Exception{wal(dir,true);try(var n=node("n1",dir)){n.start();assertEquals(1,n.statusSnapshot().appliedIndex());assertEquals(1,n.statusSnapshot().durableCommitIndex());assertEquals(NodeLifecycleState.CONTROL_ONLY,n.statusSnapshot().state());assertFalse(n.statusSnapshot().ready());}}
 @Test void corruptWalPreventsListenerBinding()throws Exception{Files.write(dir.resolve("raftkv.wal"),new byte[15]);try(var n=node("n1",dir)){assertThrows(IOException.class,n::start);assertEquals(NodeLifecycleState.FAILED,n.statusSnapshot().state());assertEquals(-1,n.boundPort());}}
 @Test void portConflictCleansUpWalAndLease()throws Exception{
  Path firstDir=dir.resolve("owner"),secondDir=dir.resolve("contender");
  try(var owner=new NodeServer(ClusterConfigTest.config(firstDir,"n1"),0)){
    owner.start();
    try(var contender=new NodeServer(ClusterConfigTest.config(secondDir,"n1"),owner.boundPort())){
       assertThrows(IOException.class,contender::start);
       assertEquals(NodeLifecycleState.FAILED,contender.statusSnapshot().state());
    }
    try(var lease=WalDirectoryLease.acquire(secondDir)){assertTrue(lease.isHeld());}
    assertThrows(IOException.class,()->WalDirectoryLease.acquire(firstDir));
  }
 }
 @Test void shutdownReleasesPortAndStorageLease()throws Exception{try(var n=node("n1",dir)){n.start();}try(var lease=WalDirectoryLease.acquire(dir)){assertTrue(lease.isHeld());}}
 @Test void shutdownClosesGrpcBeforeWal()throws Exception{var n=node("n1",dir);n.start();n.close();assertEquals(NodeLifecycleState.STOPPED,n.statusSnapshot().state());try(var lease=WalDirectoryLease.acquire(dir)){assertTrue(lease.isHeld());}}
 @Test void failedStartRetainsRootAndCleanupExceptions()throws Exception{Files.write(dir.resolve("raftkv.wal"),new byte[15]);try(var n=node("n1",dir)){var ex=assertThrows(IOException.class,n::start);assertNotNull(ex.getMessage());}}
 @Test void recoveredNodeIsNeverDataReadyInPhaseFive()throws Exception{wal(dir,true);try(var n=node("n1",dir)){n.start();assertFalse(n.statusSnapshot().ready());}}
 @Test void roleNeverChangesWhenConfiguredLeaderUnavailable()throws Exception{try(var n=node("n2",dir)){n.start();assertEquals(NodeRole.FOLLOWER,n.statusSnapshot().role());}}
 @Test void nodeSnapshotsRemainInternallyConsistentDuringClose()throws Exception{var n=node("n1",dir);n.start();var snapshot=n.statusSnapshot();n.close();assertTrue(snapshot.ready());assertEquals(0,snapshot.appliedIndex());assertFalse(n.statusSnapshot().ready());assertEquals(NodeLifecycleState.STOPPED,n.statusSnapshot().state());}
}
