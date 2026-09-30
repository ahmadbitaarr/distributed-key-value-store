package io.github.ahmadbitaarr.raftkv.wal;

import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class FileWalStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void createsWalWhenAbsent() throws Exception {
        Path wal = tempDir.resolve("node/wal.log");
        try (FileWalStore ignored = FileWalStore.open(wal)) {
            assertTrue(Files.exists(wal));
        }
    }

    @Test
    void createsMissingParentDirectories() throws Exception {
        Path wal = tempDir.resolve("a/b/c/wal.log");
        try (FileWalStore ignored = FileWalStore.open(wal)) {
            assertTrue(Files.isDirectory(wal.getParent()));
        }
    }

    @Test
    void appendEntryDurablyWritesEntry() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        try (FileWalStore store = FileWalStore.open(wal)) {
            store.appendEntryDurably(entry(1));
            assertEquals(1, store.lastDurableLogIndex());
            assertEquals(List.of(entry(1)), store.recoveryResult().entries());
        }
        assertTrue(Files.size(wal) > 0);
    }

    @Test
    void appendsMultipleEntriesDurably() throws Exception {
        try (FileWalStore store = FileWalStore.open(tempDir.resolve("wal.log"))) {
            store.appendEntryDurably(entry(1));
            store.appendEntryDurably(entry(2));
            store.appendEntryDurably(entry(3));
            assertEquals(3, store.lastDurableLogIndex());
        }
    }

    @Test
    void appendCommitDurablyAdvancesCommitIndex() throws Exception {
        try (FileWalStore store = FileWalStore.open(tempDir.resolve("wal.log"))) {
            store.appendEntryDurably(entry(1));
            store.appendCommitDurably(1);
            assertEquals(1, store.durableCommitIndex());
        }
    }

    @Test
    void equalCommitIsIdempotent() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        try (FileWalStore store = FileWalStore.open(wal)) {
            store.appendEntryDurably(entry(1));
            store.appendCommitDurably(1);
            long size = Files.size(wal);
            store.appendCommitDurably(1);
            assertEquals(size, Files.size(wal));
        }
    }

    @Test
    void rejectsRegressingCommitAppend() throws Exception {
        try (FileWalStore store = FileWalStore.open(tempDir.resolve("wal.log"))) {
            store.appendEntryDurably(entry(1));
            store.appendEntryDurably(entry(2));
            store.appendCommitDurably(2);
            assertThrows(IllegalArgumentException.class, () -> store.appendCommitDurably(1));
        }
    }

    @Test
    void rejectsCommitBeyondLastLogIndex() throws Exception {
        try (FileWalStore store = FileWalStore.open(tempDir.resolve("wal.log"))) {
            store.appendEntryDurably(entry(1));
            assertThrows(IllegalArgumentException.class, () -> store.appendCommitDurably(2));
        }
    }

    @Test
    void closeAndReopenPreservesWal() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        try (FileWalStore store = FileWalStore.open(wal)) {
            store.appendEntryDurably(entry(1));
            store.appendCommitDurably(1);
        }
        try (FileWalStore reopened = FileWalStore.open(wal)) {
            assertEquals(List.of(entry(1)), reopened.recoveryResult().entries());
            assertEquals(1, reopened.durableCommitIndex());
        }
    }

    @Test
    void reopenDoesNotTruncateValidWal() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        long size;
        try (FileWalStore store = FileWalStore.open(wal)) {
            store.appendEntryDurably(entry(1));
            size = Files.size(wal);
        }
        try (FileWalStore ignored = FileWalStore.open(wal)) {
            assertEquals(size, Files.size(wal));
        }
    }

    @Test
    void appendContinuesAfterReopen() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        try (FileWalStore store = FileWalStore.open(wal)) {
            store.appendEntryDurably(entry(1));
        }
        try (FileWalStore reopened = FileWalStore.open(wal)) {
            reopened.appendEntryDurably(entry(2));
        }
        try (FileWalStore recovered = FileWalStore.open(wal)) {
            assertEquals(List.of(entry(1), entry(2)), recovered.recoveryResult().entries());
        }
    }

    @Test
    void rejectsDirectoryAsWalPath() throws Exception {
        Path directory = Files.createDirectory(tempDir.resolve("directory"));
        assertThrows(IOException.class, () -> FileWalStore.open(directory));
    }

    @Test
    void durableAppendInvokesForceBeforeReturning() throws Exception {
        AtomicInteger forceCalls = new AtomicInteger();
        FileWalStore.ChannelForcer forcer = channel -> forceCalls.incrementAndGet();
        try (FileWalStore store = FileWalStore.openForTest(
                tempDir.resolve("wal.log"), forcer, FileWalStore.productionAtomicMover())) {
            store.appendEntryDurably(entry(1));
            assertEquals(1, forceCalls.get());
        }
    }

    @Test
    void durableCommitInvokesForceBeforeReturning() throws Exception {
        AtomicInteger forceCalls = new AtomicInteger();
        FileWalStore.ChannelForcer forcer = channel -> forceCalls.incrementAndGet();
        try (FileWalStore store = FileWalStore.openForTest(
                tempDir.resolve("wal.log"), forcer, FileWalStore.productionAtomicMover())) {
            store.appendEntryDurably(entry(1));
            int afterEntry = forceCalls.get();
            store.appendCommitDurably(1);
            assertEquals(afterEntry + 1, forceCalls.get());
        }
    }

    @Test
    void canonicalRewritePreservesEntries() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        try (FileWalStore store = FileWalStore.open(wal)) {
            store.appendEntryDurably(entry(1));
            store.appendEntryDurably(entry(2));
            store.appendEntryDurably(entry(3));
            store.rewriteDurably(List.of(entry(1), entry(2)), 0);
            assertEquals(List.of(entry(1), entry(2)), store.recoveryResult().entries());
        }
    }

    @Test
    void canonicalRewritePreservesCommitIndex() throws Exception {
        try (FileWalStore store = FileWalStore.open(tempDir.resolve("wal.log"))) {
            store.appendEntryDurably(entry(1));
            store.appendEntryDurably(entry(2));
            store.appendEntryDurably(entry(3));
            store.appendCommitDurably(2);
            store.rewriteDurably(List.of(entry(1), entry(2)), 2);
            assertEquals(2, store.durableCommitIndex());
        }
    }

    @Test
    void canonicalRewriteRejectsCommitBeyondRetainedLog() throws Exception {
        try (FileWalStore store = FileWalStore.open(tempDir.resolve("wal.log"))) {
            store.appendEntryDurably(entry(1));
            store.appendEntryDurably(entry(2));
            store.appendCommitDurably(2);
            assertThrows(IllegalStateException.class,
                    () -> store.rewriteDurably(List.of(entry(1)), 2));
        }
    }

    @Test
    void rewriteClosesAndReopensCorrectly() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        try (FileWalStore store = FileWalStore.open(wal)) {
            store.appendEntryDurably(entry(1));
            store.appendEntryDurably(entry(2));
            store.rewriteDurably(List.of(entry(1)), 0);
            store.appendEntryDurably(entry(2));
            assertEquals(2, store.lastDurableLogIndex());
        }
    }

    @Test
    void failedAtomicReplacementDoesNotIntentionallyDestroyOriginalWal() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        FileWalStore.AtomicMover failingMover = (source, target) -> {
            throw new AtomicMoveNotSupportedException(source.toString(), target.toString(), "simulated");
        };
        try (FileWalStore store = FileWalStore.openForTest(
                wal, FileWalStore.productionForcer(), failingMover)) {
            store.appendEntryDurably(entry(1));
            store.appendEntryDurably(entry(2));
            assertThrows(AtomicMoveNotSupportedException.class,
                    () -> store.rewriteDurably(List.of(entry(1)), 0));
            assertTrue(Files.exists(wal));
            assertEquals(2, store.lastDurableLogIndex());
        }
        try (FileWalStore reopened = FileWalStore.open(wal)) {
            assertEquals(List.of(entry(1), entry(2)), reopened.recoveryResult().entries());
        }
    }

    @Test
    void orphanRewriteTempIsNotAuthoritativeOnStartup() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        try (FileWalStore store = FileWalStore.open(wal)) {
            store.appendEntryDurably(entry(1));
        }
        Files.write(FileWalStore.rewriteTempPath(wal), new byte[]{1, 2, 3, 4});
        try (FileWalStore reopened = FileWalStore.open(wal)) {
            assertEquals(List.of(entry(1)), reopened.recoveryResult().entries());
        }
    }

    @Test
    void normalRecoverySucceedsWhenOrphanRewriteTempExists() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        try (FileWalStore store = FileWalStore.open(wal)) {
            store.appendEntryDurably(entry(1));
            store.appendEntryDurably(entry(2));
        }
        Path orphan = FileWalStore.rewriteTempPath(wal);
        Files.writeString(orphan, "not authoritative");
        try (FileWalStore reopened = FileWalStore.open(wal)) {
            assertEquals(2, reopened.lastDurableLogIndex());
        }
    }

    private static LogEntry entry(long index) {
        return new LogEntry(index, "request-" + index,
                Command.put("key-" + index, new byte[]{(byte) index}),
                new byte[]{(byte) (index + 10)});
    }
}
