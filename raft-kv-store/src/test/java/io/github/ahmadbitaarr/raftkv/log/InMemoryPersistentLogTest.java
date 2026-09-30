package io.github.ahmadbitaarr.raftkv.log;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryPersistentLogTest {

    @Test
    void emptyLogReportsLastLogIndexZero() {
        InMemoryPersistentLog log = new InMemoryPersistentLog();

        assertEquals(0, log.lastLogIndex());
    }

    @Test
    void firstAppendUsesIndexOne() {
        InMemoryPersistentLog log = new InMemoryPersistentLog();
        LogEntry entry = entry(1, "one");

        log.append(entry);

        assertEquals(1, log.lastLogIndex());
        assertEquals(entry, log.get(1).orElseThrow());
    }

    @Test
    void appendsSequentialEntries() {
        InMemoryPersistentLog log = new InMemoryPersistentLog();

        log.append(entry(1, "one"));
        log.append(entry(2, "two"));
        log.append(entry(3, "three"));

        assertEquals(3, log.lastLogIndex());
    }

    @Test
    void looksUpEntryByIndex() {
        InMemoryPersistentLog log = logWithEntries(4);

        assertEquals(entry(2, "entry-2"), log.get(2).orElseThrow());
    }

    @Test
    void missingFutureIndexReturnsEmpty() {
        InMemoryPersistentLog log = logWithEntries(3);

        assertTrue(log.get(4).isEmpty());
        assertTrue(log.get(Long.MAX_VALUE).isEmpty());
    }

    @Test
    void rejectsNonPositiveLookupIndex() {
        InMemoryPersistentLog log = new InMemoryPersistentLog();

        assertThrows(IllegalArgumentException.class, () -> log.get(0));
        assertThrows(IllegalArgumentException.class, () -> log.get(-1));
    }

    @Test
    void retrievesOrderedRange() {
        InMemoryPersistentLog log = logWithEntries(5);

        List<LogEntry> range = log.getRange(2, 4);

        assertEquals(List.of(
                entry(2, "entry-2"),
                entry(3, "entry-3"),
                entry(4, "entry-4")), range);
    }

    @Test
    void retrievesSingleEntryRange() {
        InMemoryPersistentLog log = logWithEntries(3);

        assertEquals(List.of(entry(3, "entry-3")), log.getRange(3, 3));
    }

    @Test
    void returnedRangeCannotModifyLog() {
        InMemoryPersistentLog log = logWithEntries(3);
        List<LogEntry> range = log.getRange(1, 3);

        assertThrows(UnsupportedOperationException.class, () -> range.remove(0));
        assertEquals(3, log.lastLogIndex());
        assertEquals(entry(1, "entry-1"), log.get(1).orElseThrow());
    }

    @Test
    void rejectsIndexGap() {
        InMemoryPersistentLog log = new InMemoryPersistentLog();
        log.append(entry(1, "one"));

        assertThrows(IllegalArgumentException.class,
                () -> log.append(entry(3, "three")));
    }

    @Test
    void rejectsDuplicateIndex() {
        InMemoryPersistentLog log = new InMemoryPersistentLog();
        log.append(entry(1, "one"));

        assertThrows(IllegalArgumentException.class,
                () -> log.append(entry(1, "replacement")));
    }

    @Test
    void rejectsOutOfOrderIndex() {
        InMemoryPersistentLog log = logWithEntries(3);

        assertThrows(IllegalArgumentException.class,
                () -> log.append(entry(2, "two-again")));
    }

    @Test
    void rejectsNullEntry() {
        InMemoryPersistentLog log = new InMemoryPersistentLog();

        assertThrows(NullPointerException.class, () -> log.append(null));
    }

    @Test
    void rejectsInvalidRangeIndexes() {
        InMemoryPersistentLog log = logWithEntries(3);

        assertThrows(IllegalArgumentException.class, () -> log.getRange(0, 1));
        assertThrows(IllegalArgumentException.class, () -> log.getRange(-1, 1));
        assertThrows(IllegalArgumentException.class, () -> log.getRange(1, 0));
        assertThrows(IllegalArgumentException.class, () -> log.getRange(1, -1));
    }

    @Test
    void rejectsReversedRange() {
        InMemoryPersistentLog log = logWithEntries(4);

        assertThrows(IllegalArgumentException.class, () -> log.getRange(4, 2));
    }

    @Test
    void rejectsRangeBeyondLastLogIndex() {
        InMemoryPersistentLog log = logWithEntries(3);

        assertThrows(IllegalArgumentException.class, () -> log.getRange(2, 4));
    }

    @Test
    void truncatesSuffix() {
        InMemoryPersistentLog log = logWithEntries(5);
        LogEntry first = log.get(1).orElseThrow();
        LogEntry second = log.get(2).orElseThrow();
        LogEntry third = log.get(3).orElseThrow();

        log.truncateAfter(3);

        assertEquals(3, log.lastLogIndex());
        assertEquals(first, log.get(1).orElseThrow());
        assertEquals(second, log.get(2).orElseThrow());
        assertEquals(third, log.get(3).orElseThrow());
        assertTrue(log.get(4).isEmpty());
        assertTrue(log.get(5).isEmpty());
    }

    @Test
    void truncateAtLastIndexIsNoOp() {
        InMemoryPersistentLog log = logWithEntries(3);
        List<LogEntry> before = log.getRange(1, 3);

        log.truncateAfter(3);

        assertEquals(3, log.lastLogIndex());
        assertEquals(before, log.getRange(1, 3));
    }

    @Test
    void truncateBeyondLastIndexIsNoOp() {
        InMemoryPersistentLog log = logWithEntries(3);
        List<LogEntry> before = log.getRange(1, 3);

        log.truncateAfter(10);

        assertEquals(3, log.lastLogIndex());
        assertEquals(before, log.getRange(1, 3));
    }

    @Test
    void truncateEmptyLogIsNoOp() {
        InMemoryPersistentLog log = new InMemoryPersistentLog();

        log.truncateAfter(0);
        log.truncateAfter(10);

        assertEquals(0, log.lastLogIndex());
    }

    @Test
    void truncateToZeroClearsLog() {
        InMemoryPersistentLog log = logWithEntries(3);

        log.truncateAfter(0);

        assertEquals(0, log.lastLogIndex());
        assertTrue(log.get(1).isEmpty());
        assertTrue(log.get(2).isEmpty());
        assertTrue(log.get(3).isEmpty());
    }

    @Test
    void rejectsNegativeTruncationIndex() {
        InMemoryPersistentLog log = new InMemoryPersistentLog();

        assertThrows(IllegalArgumentException.class, () -> log.truncateAfter(-1));
    }

    @Test
    void appendsCorrectlyAfterTruncation() {
        InMemoryPersistentLog log = logWithEntries(4);
        LogEntry replacement = entry(3, "replacement-three");

        log.truncateAfter(2);
        log.append(replacement);

        assertEquals(3, log.lastLogIndex());
        assertEquals(replacement, log.get(3).orElseThrow());
    }

    @Test
    void rejectsGapAfterTruncation() {
        InMemoryPersistentLog log = logWithEntries(4);

        log.truncateAfter(2);

        assertThrows(IllegalArgumentException.class,
                () -> log.append(entry(4, "four-again")));
    }

    @Test
    void lastLogIndexTracksTruncationCorrectly() {
        InMemoryPersistentLog log = new InMemoryPersistentLog();
        assertEquals(0, log.lastLogIndex());

        log.append(entry(1, "one"));
        assertEquals(1, log.lastLogIndex());

        log.append(entry(2, "two"));
        assertEquals(2, log.lastLogIndex());

        log.append(entry(3, "three"));
        assertEquals(3, log.lastLogIndex());

        log.truncateAfter(1);
        assertEquals(1, log.lastLogIndex());

        log.append(entry(2, "new-two"));
        assertEquals(2, log.lastLogIndex());
    }

    private static InMemoryPersistentLog logWithEntries(int count) {
        InMemoryPersistentLog log = new InMemoryPersistentLog();
        for (int index = 1; index <= count; index++) {
            log.append(entry(index, "entry-" + index));
        }
        return log;
    }

    private static LogEntry entry(long index, String marker) {
        return new LogEntry(
                index,
                "request-" + marker,
                Command.put("key-" + marker, new byte[]{(byte) index}),
                new byte[]{(byte) (index + 10), (byte) (index + 20)});
    }
}
