package io.github.ahmadbitaarr.raftkv.log;

import io.github.ahmadbitaarr.raftkv.wal.FileWalStore;
import io.github.ahmadbitaarr.raftkv.wal.WalRecoveryResult;
import io.github.ahmadbitaarr.raftkv.wal.WalStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WalBackedPersistentLogTest {
    @TempDir
    Path tempDir;

    @Test
    void emptyWalBackedLogReportsLastIndexZero() throws Exception {
        try (WalBackedPersistentLog log = open()) {
            assertEquals(0, log.lastLogIndex());
        }
    }

    @Test
    void firstAppendPersistsIndexOne() throws Exception {
        Path wal = walPath();
        try (WalBackedPersistentLog log = WalBackedPersistentLog.open(wal)) {
            log.append(entry(1, "one"));
        }
        try (WalBackedPersistentLog reopened = WalBackedPersistentLog.open(wal)) {
            assertEquals(entry(1, "one"), reopened.get(1).orElseThrow());
        }
    }

    @Test
    void sequentialAppendPreservesPhase2Semantics() throws Exception {
        try (WalBackedPersistentLog log = open()) {
            log.append(entry(1, "one"));
            log.append(entry(2, "two"));
            log.append(entry(3, "three"));
            assertEquals(3, log.lastLogIndex());
        }
    }

    @Test
    void rejectsIndexGapBeforeWritingWal() throws Exception {
        try (WalBackedPersistentLog log = open()) {
            log.append(entry(1, "one"));
            assertThrows(IllegalArgumentException.class, () -> log.append(entry(3, "three")));
            assertEquals(1, log.lastLogIndex());
        }
    }

    @Test
    void rejectsDuplicateIndexBeforeWritingWal() throws Exception {
        try (WalBackedPersistentLog log = open()) {
            log.append(entry(1, "one"));
            assertThrows(IllegalArgumentException.class, () -> log.append(entry(1, "replacement")));
            assertEquals(1, log.lastLogIndex());
        }
    }

    @Test
    void lookupByIndexWorks() throws Exception {
        try (WalBackedPersistentLog log = logWithEntries(3)) {
            assertEquals(entry(2, "entry-2"), log.get(2).orElseThrow());
        }
    }

    @Test
    void orderedRangeWorks() throws Exception {
        try (WalBackedPersistentLog log = logWithEntries(4)) {
            assertEquals(List.of(entry(2, "entry-2"), entry(3, "entry-3")), log.getRange(2, 3));
        }
    }

    @Test
    void lastLogIndexTracksContents() throws Exception {
        try (WalBackedPersistentLog log = open()) {
            assertEquals(0, log.lastLogIndex());
            log.append(entry(1, "one"));
            assertEquals(1, log.lastLogIndex());
            log.append(entry(2, "two"));
            assertEquals(2, log.lastLogIndex());
        }
    }

    @Test
    void appendSurvivesCloseAndReopen() throws Exception {
        Path wal = walPath();
        try (WalBackedPersistentLog log = WalBackedPersistentLog.open(wal)) {
            log.append(entry(1, "one"));
            log.append(entry(2, "two"));
        }
        try (WalBackedPersistentLog reopened = WalBackedPersistentLog.open(wal)) {
            assertEquals(2, reopened.lastLogIndex());
        }
    }

    @Test
    void lookupWorksAfterRecovery() throws Exception {
        Path wal = walPath();
        try (WalBackedPersistentLog log = WalBackedPersistentLog.open(wal)) {
            log.append(entry(1, "one"));
            log.append(entry(2, "two"));
        }
        try (WalBackedPersistentLog reopened = WalBackedPersistentLog.open(wal)) {
            assertEquals(entry(2, "two"), reopened.get(2).orElseThrow());
        }
    }

    @Test
    void rangeWorksAfterRecovery() throws Exception {
        Path wal = walPath();
        try (WalBackedPersistentLog log = WalBackedPersistentLog.open(wal)) {
            log.append(entry(1, "one"));
            log.append(entry(2, "two"));
            log.append(entry(3, "three"));
        }
        try (WalBackedPersistentLog reopened = WalBackedPersistentLog.open(wal)) {
            assertEquals(List.of(entry(1, "one"), entry(2, "two")), reopened.getRange(1, 2));
        }
    }

    @Test
    void lastLogIndexRestoresAfterRecovery() throws Exception {
        Path wal = walPath();
        try (WalBackedPersistentLog log = WalBackedPersistentLog.open(wal)) {
            log.append(entry(1, "one"));
            log.append(entry(2, "two"));
        }
        try (WalBackedPersistentLog reopened = WalBackedPersistentLog.open(wal)) {
            assertEquals(2, reopened.lastLogIndex());
        }
    }

    @Test
    void structuralTruncateRemovesSuffix() throws Exception {
        try (WalBackedPersistentLog log = logWithEntries(4)) {
            log.truncateAfter(2);
            assertEquals(2, log.lastLogIndex());
            assertTrue(log.get(3).isEmpty());
        }
    }

    @Test
    void structuralTruncateToZeroClearsLog() throws Exception {
        try (WalBackedPersistentLog log = logWithEntries(3)) {
            log.truncateAfter(0);
            assertEquals(0, log.lastLogIndex());
        }
    }

    @Test
    void truncateAtLastIndexIsNoOp() throws Exception {
        try (WalBackedPersistentLog log = logWithEntries(3)) {
            log.truncateAfter(3);
            assertEquals(3, log.lastLogIndex());
        }
    }

    @Test
    void truncateBeyondLastIndexIsNoOp() throws Exception {
        try (WalBackedPersistentLog log = logWithEntries(3)) {
            log.truncateAfter(20);
            assertEquals(3, log.lastLogIndex());
        }
    }

    @Test
    void appendAfterTruncationUsesNextContiguousIndex() throws Exception {
        try (WalBackedPersistentLog log = logWithEntries(4)) {
            log.truncateAfter(2);
            LogEntry replacement = entry(3, "replacement");
            log.append(replacement);
            assertEquals(replacement, log.get(3).orElseThrow());
        }
    }

    @Test
    void appendAfterTruncationAndReopenRemainsCorrect() throws Exception {
        Path wal = walPath();
        try (WalBackedPersistentLog log = WalBackedPersistentLog.open(wal)) {
            appendEntries(log, 4);
            log.truncateAfter(2);
            log.append(entry(3, "replacement"));
        }
        try (WalBackedPersistentLog reopened = WalBackedPersistentLog.open(wal)) {
            assertEquals(3, reopened.lastLogIndex());
            assertEquals(entry(3, "replacement"), reopened.get(3).orElseThrow());
        }
    }

    @Test
    void truncationRewritesWalConsistently() throws Exception {
        Path wal = walPath();
        try (WalBackedPersistentLog log = WalBackedPersistentLog.open(wal)) {
            appendEntries(log, 4);
            log.truncateAfter(2);
        }
        try (WalBackedPersistentLog reopened = WalBackedPersistentLog.open(wal)) {
            assertEquals(2, reopened.lastLogIndex());
            assertTrue(reopened.get(3).isEmpty());
        }
    }

    @Test
    void truncationPreservesDurableCommitMetadataWhenValid() throws Exception {
        Path wal = walPath();
        try (FileWalStore store = FileWalStore.open(wal)) {
            store.appendEntryDurably(entry(1, "one"));
            store.appendEntryDurably(entry(2, "two"));
            store.appendEntryDurably(entry(3, "three"));
            store.appendCommitDurably(2);
        }
        try (WalBackedPersistentLog log = WalBackedPersistentLog.open(wal)) {
            log.truncateAfter(2);
        }
        try (FileWalStore store = FileWalStore.open(wal)) {
            assertEquals(2, store.durableCommitIndex());
            assertEquals(2, store.lastDurableLogIndex());
        }
    }

    @Test
    void truncateBelowDurableCommitFailsInvariantGuard() throws Exception {
        Path wal = walPath();
        try (FileWalStore store = FileWalStore.open(wal)) {
            store.appendEntryDurably(entry(1, "one"));
            store.appendEntryDurably(entry(2, "two"));
            store.appendCommitDurably(2);
        }
        try (WalBackedPersistentLog log = WalBackedPersistentLog.open(wal)) {
            assertThrows(IllegalStateException.class, () -> log.truncateAfter(1));
            assertEquals(2, log.lastLogIndex());
        }
    }

    @Test
    void failedDurableAppendDoesNotUpdateLogicalMemory() throws Exception {
        try (WalBackedPersistentLog log = new WalBackedPersistentLog(new FailingAppendWalStore())) {
            assertThrows(IOException.class, () -> log.append(entry(1, "one")));
            assertEquals(0, log.lastLogIndex());
        }
    }

    @Test
    void failedAtomicReplacementLeavesInMemoryStateUnchanged() throws Exception {
        try (WalBackedPersistentLog log = new WalBackedPersistentLog(new FailingRewriteWalStore())) {
            log.append(entry(1, "one"));
            log.append(entry(2, "two"));
            assertThrows(AtomicMoveNotSupportedException.class, () -> log.truncateAfter(1));
            assertEquals(2, log.lastLogIndex());
            assertEquals(entry(2, "two"), log.get(2).orElseThrow());
        }
    }

    private static class MemoryWalStore implements WalStore {
        final java.util.ArrayList<LogEntry> entries = new java.util.ArrayList<>();

        @Override
        public WalRecoveryResult recoveryResult() {
            return new WalRecoveryResult(entries, 0, 0, 0, false);
        }

        @Override
        public void appendEntryDurably(LogEntry entry) throws IOException {
            entries.add(entry);
        }

        @Override
        public void appendCommitDurably(long commitIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rewriteDurably(List<LogEntry> retainedEntries, long commitIndex) throws IOException {
            entries.clear();
            entries.addAll(retainedEntries);
        }

        @Override
        public long lastDurableLogIndex() {
            return entries.size();
        }

        @Override
        public long durableCommitIndex() {
            return 0;
        }

        @Override
        public void close() {
        }
    }

    private static final class FailingAppendWalStore extends MemoryWalStore {
        @Override
        public void appendEntryDurably(LogEntry entry) throws IOException {
            throw new IOException("simulated durable append failure");
        }
    }

    private static final class FailingRewriteWalStore extends MemoryWalStore {
        @Override
        public void rewriteDurably(List<LogEntry> retainedEntries, long commitIndex)
                throws IOException {
            throw new AtomicMoveNotSupportedException("temp", "wal", "simulated");
        }
    }

    private WalBackedPersistentLog open() throws Exception {
        return WalBackedPersistentLog.open(walPath());
    }

    private WalBackedPersistentLog logWithEntries(int count) throws Exception {
        WalBackedPersistentLog log = open();
        appendEntries(log, count);
        return log;
    }

    private static void appendEntries(WalBackedPersistentLog log, int count) throws Exception {
        for (int i = 1; i <= count; i++) {
            log.append(entry(i, "entry-" + i));
        }
    }

    private Path walPath() {
        return tempDir.resolve("wal.log");
    }

    private static LogEntry entry(long index, String label) {
        return new LogEntry(index, "request-" + label,
                Command.put("key-" + label, new byte[]{(byte) index}),
                new byte[]{(byte) (index + 30)});
    }
}
