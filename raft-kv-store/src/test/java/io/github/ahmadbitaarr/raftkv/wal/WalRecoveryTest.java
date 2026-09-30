package io.github.ahmadbitaarr.raftkv.wal;

import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WalRecoveryTest {
    @TempDir
    Path tempDir;

    private final WalRecordCodec codec = new WalRecordCodec();

    @Test
    void emptyWalRecoversEmptyState() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        Files.createFile(wal);
        try (FileWalStore store = FileWalStore.open(wal)) {
            WalRecoveryResult result = store.recoveryResult();
            assertTrue(result.entries().isEmpty());
            assertEquals(0, result.commitIndex());
            assertEquals(0, result.lastValidOffset());
            assertFalse(result.tailTruncated());
        }
    }

    @Test
    void validWalRecoversAllEntries() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        writeFrames(wal, entryFrame(1), entryFrame(2), entryFrame(3));
        try (FileWalStore store = FileWalStore.open(wal)) {
            assertEquals(List.of(entry(1), entry(2), entry(3)), store.recoveryResult().entries());
        }
    }

    @Test
    void highestValidCommitIndexIsRecovered() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        writeFrames(wal, entryFrame(1), entryFrame(2), commitFrame(1), commitFrame(2));
        try (FileWalStore store = FileWalStore.open(wal)) {
            assertEquals(2, store.recoveryResult().commitIndex());
        }
    }

    @Test
    void validUncommittedTailEntryIsPreserved() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        writeFrames(wal, entryFrame(1), commitFrame(1), entryFrame(2));
        try (FileWalStore store = FileWalStore.open(wal)) {
            assertEquals(2, store.lastDurableLogIndex());
            assertEquals(1, store.durableCommitIndex());
        }
    }

    @Test
    void incompleteFinalHeaderIsTruncated() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        byte[] valid = entryFrame(1);
        writeFrames(wal, valid, new byte[]{'R', 'K', 'V'});
        long expected = valid.length;
        try (FileWalStore store = FileWalStore.open(wal)) {
            assertTrue(store.recoveryResult().tailTruncated());
            assertEquals(expected, Files.size(wal));
        }
    }

    @Test
    void incompleteFinalPayloadIsTruncated() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        byte[] first = entryFrame(1);
        byte[] second = entryFrame(2);
        writeFrames(wal, first, Arrays.copyOf(second, WalRecordCodec.HEADER_SIZE + 2));
        try (FileWalStore store = FileWalStore.open(wal)) {
            assertTrue(store.recoveryResult().tailTruncated());
            assertEquals(first.length, Files.size(wal));
        }
    }

    @Test
    void incompleteFinalChecksumIsTruncated() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        byte[] first = entryFrame(1);
        byte[] second = entryFrame(2);
        writeFrames(wal, first, Arrays.copyOf(second, second.length - 2));
        try (FileWalStore store = FileWalStore.open(wal)) {
            assertTrue(store.recoveryResult().tailTruncated());
            assertEquals(first.length, Files.size(wal));
        }
    }

    @Test
    void recoveryResultReportsTailTruncation() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        byte[] valid = entryFrame(1);
        writeFrames(wal, valid, new byte[]{1, 2});
        long originalLength = Files.size(wal);
        try (FileWalStore store = FileWalStore.open(wal)) {
            WalRecoveryResult result = store.recoveryResult();
            assertTrue(result.tailTruncated());
            assertEquals(originalLength, result.originalFileLength());
            assertEquals(valid.length, result.lastValidOffset());
        }
    }

    @Test
    void repairedTailAllowsSubsequentAppend() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        byte[] first = entryFrame(1);
        writeFrames(wal, first, new byte[]{1, 2, 3});
        try (FileWalStore store = FileWalStore.open(wal)) {
            store.appendEntryDurably(entry(2));
        }
        try (FileWalStore recovered = FileWalStore.open(wal)) {
            assertEquals(List.of(entry(1), entry(2)), recovered.recoveryResult().entries());
        }
    }

    @Test
    void checksumMismatchOnFinalUncommittedRecordFailsConservatively() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        byte[] bad = entryFrame(2);
        bad[WalRecordCodec.HEADER_SIZE + 1] ^= 1;
        writeFrames(wal, entryFrame(1), bad);
        assertThrows(WalCorruptionException.class, () -> FileWalStore.open(wal));
    }

    @Test
    void checksumMismatchInsideEarlierHistoryFails() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        byte[] bad = entryFrame(1);
        bad[WalRecordCodec.HEADER_SIZE + 1] ^= 1;
        writeFrames(wal, bad, entryFrame(2));
        assertThrows(WalCorruptionException.class, () -> FileWalStore.open(wal));
    }

    @Test
    void corruptionInsideCommittedHistoryFails() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        byte[] bad = entryFrame(1);
        bad[bad.length - 1] ^= 1;
        writeFrames(wal, bad, commitFrame(1));
        assertThrows(WalCorruptionException.class, () -> FileWalStore.open(wal));
    }

    @Test
    void badMagicInCompleteRecordFails() throws Exception {
        byte[] frame = entryFrame(1);
        frame[0] = 'X';
        WalRecordCodec.rewriteChecksumForTest(frame);
        assertCorrupt(frame);
    }

    @Test
    void unsupportedVersionInCompleteRecordFails() throws Exception {
        byte[] frame = entryFrame(1);
        frame[4] = 2;
        WalRecordCodec.rewriteChecksumForTest(frame);
        assertCorrupt(frame);
    }

    @Test
    void invalidRecordTypeInCompleteRecordFails() throws Exception {
        byte[] frame = entryFrame(1);
        frame[5] = 99;
        WalRecordCodec.rewriteChecksumForTest(frame);
        assertCorrupt(frame);
    }

    @Test
    void oversizedLengthInCompleteHeaderFails() throws Exception {
        byte[] frame = entryFrame(1);
        ByteBuffer.wrap(frame).order(ByteOrder.BIG_ENDIAN).putInt(6, WalRecordCodec.MAX_PAYLOAD_SIZE + 1);
        assertCorrupt(frame);
    }

    @Test
    void malformedCompletePayloadFails() throws Exception {
        assertCorrupt(WalRecordCodec.frameForTest(WalRecordType.ENTRY, new byte[]{1, 2, 3}));
    }

    @Test
    void commitBeyondLastLogIndexFails() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        writeFrames(wal, entryFrame(1), commitFrame(2));
        assertThrows(WalCorruptionException.class, () -> FileWalStore.open(wal));
    }

    @Test
    void nonContiguousRecoveredEntryIndexesFail() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        writeFrames(wal, entryFrame(1), entryFrame(3));
        assertThrows(WalCorruptionException.class, () -> FileWalStore.open(wal));
    }

    @Test
    void duplicateRecoveredEntryIndexFails() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        writeFrames(wal, entryFrame(1), entryFrame(1));
        assertThrows(WalCorruptionException.class, () -> FileWalStore.open(wal));
    }

    @Test
    void outOfOrderRecoveredEntryIndexFails() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        writeFrames(wal, entryFrame(1), entryFrame(2), entryFrame(1));
        assertThrows(WalCorruptionException.class, () -> FileWalStore.open(wal));
    }

    @Test
    void regressingCommitRecordFails() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        writeFrames(wal, entryFrame(1), entryFrame(2), commitFrame(2), commitFrame(1));
        assertThrows(WalCorruptionException.class, () -> FileWalStore.open(wal));
    }

    @Test
    void equalCommitRecordsAreHarmless() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        writeFrames(wal, entryFrame(1), commitFrame(1), commitFrame(1));
        try (FileWalStore store = FileWalStore.open(wal)) {
            assertEquals(1, store.durableCommitIndex());
        }
    }

    @Test
    void cleanEofDoesNotModifyWal() throws Exception {
        Path wal = tempDir.resolve("wal.log");
        writeFrames(wal, entryFrame(1), entryFrame(2));
        byte[] before = Files.readAllBytes(wal);
        try (FileWalStore ignored = FileWalStore.open(wal)) {
            assertArrayEquals(before, Files.readAllBytes(wal));
        }
    }

    private void assertCorrupt(byte[] frame) throws Exception {
        Path wal = tempDir.resolve("corrupt-" + System.nanoTime() + ".log");
        Files.write(wal, frame);
        assertThrows(WalCorruptionException.class, () -> FileWalStore.open(wal));
    }

    private byte[] entryFrame(long index) throws Exception {
        return codec.encode(WalRecord.entry(entry(index)));
    }

    private byte[] commitFrame(long index) throws Exception {
        return codec.encode(WalRecord.commit(index));
    }

    private static void writeFrames(Path path, byte[]... frames) throws Exception {
        Files.deleteIfExists(path);
        Files.createFile(path);
        for (byte[] frame : frames) {
            Files.write(path, frame, StandardOpenOption.APPEND);
        }
    }

    private static LogEntry entry(long index) {
        return new LogEntry(index, "request-" + index,
                Command.put("key-" + index, new byte[]{(byte) index}),
                new byte[]{(byte) (index + 20)});
    }
}
