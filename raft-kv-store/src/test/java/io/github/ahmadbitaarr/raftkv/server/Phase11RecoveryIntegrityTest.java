package io.github.ahmadbitaarr.raftkv.server;

import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.leader.CanonicalCommandFingerprint;
import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.wal.FileWalStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

/** Invalid committed request metadata is a recovery-authority failure, not a cache miss. */
class Phase11RecoveryIntegrityTest {
    @TempDir Path root;
    private ClusterConfig cfg(){return ClusterConfigTest.config(root.resolve("n1"),"n1");}
    @Test void committedFingerprintMismatchPreventsNodeServerReadinessAndDataListener()throws Exception {
        var c=Command.put("key",new byte[]{1});
        try(var wal=FileWalStore.open(cfg().walFile())){
            wal.appendEntryDurably(new LogEntry(1,"committed",c,new byte[]{1,2,3}));
            wal.appendCommitDurably(1);
        }
        try(var leader=new NodeServer(cfg(),0)){
            assertThrows(IOException.class,leader::start);
            assertEquals(NodeLifecycleState.FAILED,leader.statusSnapshot().state());
            assertFalse(leader.statusSnapshot().ready());
            assertEquals(-1,leader.boundPort());
        }
        try(var wal=FileWalStore.open(cfg().walFile())){
            assertEquals(1,wal.durableCommitIndex());
            assertEquals(1,wal.lastDurableLogIndex());
        }
    }
    @Test void duplicateCommittedIdsAtDifferentIndexesPreventRecoveryAuthorization()throws Exception {
        var a=Command.put("key",new byte[]{1});var b=Command.delete("key");
        try(var wal=FileWalStore.open(cfg().walFile())){
            wal.appendEntryDurably(new LogEntry(1,"same-id",a,CanonicalCommandFingerprint.of(a)));
            wal.appendEntryDurably(new LogEntry(2,"same-id",b,CanonicalCommandFingerprint.of(b)));
            wal.appendCommitDurably(2);
        }
        try(var leader=new NodeServer(cfg(),0)){
            assertThrows(IOException.class,leader::start);
            assertEquals(NodeLifecycleState.FAILED,leader.statusSnapshot().state());
            assertFalse(leader.statusSnapshot().ready());
            assertEquals(-1,leader.boundPort());
        }
        try(var wal=FileWalStore.open(cfg().walFile())){
            assertEquals(2,wal.durableCommitIndex());
        }
    }
}
