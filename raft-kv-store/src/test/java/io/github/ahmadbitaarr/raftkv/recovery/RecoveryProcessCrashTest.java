package io.github.ahmadbitaarr.raftkv.recovery;

import io.github.ahmadbitaarr.raftkv.log.WalBackedPersistentLog;
import io.github.ahmadbitaarr.raftkv.statemachine.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** Real separate JVM exits, not clean WalStore.close(): validates already-forced boundaries. */
class RecoveryProcessCrashTest {
    @TempDir Path root;
    private Path crash(String mode)throws Exception {
        Path path=root.resolve(mode).resolve("raftkv.wal");
        String executable=Path.of(System.getProperty("java.home"),"bin",
                System.getProperty("os.name").toLowerCase().contains("win")?"java.exe":"java")
                .toString();
        var child=new ProcessBuilder(executable,"-cp",System.getProperty("java.class.path"),
                RecoveryCrashProbe.class.getName(),mode,path.toString()).redirectErrorStream(true).start();
        boolean done=child.waitFor(8,TimeUnit.SECONDS);
        if(!done){child.destroyForcibly();fail("crash boundary process did not exit");}
        assertEquals(0,child.exitValue());
        return path;
    }
    @Test void hardCrashAfterEntryForceBeforeCommitReplaysNoUncommittedMutation()throws Exception {
        Path file=crash("ENTRY_ONLY");
        try(var wal=FileWalStore.open(file)) {
            assertEquals(1,wal.lastDurableLogIndex());assertEquals(0,wal.durableCommitIndex());
            var applier=new StateMachineApplier(new KvStateMachine());
            assertEquals(0,RecoveryManager.recoverLocal(new WalBackedPersistentLog(wal),wal,applier).appliedIndex());
            assertTrue(applier.diagnosticGetForTest("crash").isEmpty());
            assertEquals(0,wal.lastDurableLogIndex());
        }
    }
    @Test void hardCrashAfterCommitForceWithoutResponseReconstructsCommittedBytes()throws Exception {
        Path file=crash("COMMIT_ONLY");
        try(var wal=FileWalStore.open(file)) {
            assertEquals(1,wal.durableCommitIndex());
            var applier=new StateMachineApplier(new KvStateMachine());
            assertEquals(1,RecoveryManager.recoverLocal(new WalBackedPersistentLog(wal),wal,applier).appliedIndex());
            assertArrayEquals(new byte[]{9,0,4},applier.diagnosticGetForTest("crash").orElseThrow());
        }
    }
    @Test void hardCrashAfterInMemoryApplicationReconstructsIdenticalStateOnRestart()throws Exception {
        Path file=crash("AFTER_APPLY");
        try(var wal=FileWalStore.open(file)) {
            var applier=new StateMachineApplier(new KvStateMachine());
            RecoveryManager.recoverLocal(new WalBackedPersistentLog(wal),wal,applier);
            assertEquals(1,applier.appliedIndex());
            assertArrayEquals(new byte[]{9,0,4},applier.diagnosticGetForTest("crash").orElseThrow());
        }
    }
}
