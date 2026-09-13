package io.github.ahmadbitaarr.raftkv.log;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LogEntryTest {

    @Test
    void createsValidLogEntry() {
        Command command = Command.put("alpha", new byte[]{1, 2, 3});
        byte[] fingerprint = new byte[]{10, 20, 30};

        LogEntry entry = new LogEntry(1, "request-1", command, fingerprint);

        assertEquals(1, entry.index());
        assertEquals("request-1", entry.requestId());
        assertSame(command, entry.command());
        assertArrayEquals(fingerprint, entry.commandFingerprint());
    }

    @Test
    void requiresPositiveIndex() {
        Command command = Command.delete("alpha");
        byte[] fingerprint = new byte[]{1};

        assertThrows(IllegalArgumentException.class,
                () -> new LogEntry(0, "request-1", command, fingerprint));
        assertThrows(IllegalArgumentException.class,
                () -> new LogEntry(-1, "request-1", command, fingerprint));
    }

    @Test
    void rejectsNullCommand() {
        assertThrows(NullPointerException.class,
                () -> new LogEntry(1, "request-1", null, new byte[]{1}));
    }

    @Test
    void rejectsInvalidRequestId() {
        Command command = Command.delete("alpha");
        byte[] fingerprint = new byte[]{1};

        assertThrows(NullPointerException.class,
                () -> new LogEntry(1, null, command, fingerprint));
        assertThrows(IllegalArgumentException.class,
                () -> new LogEntry(1, "", command, fingerprint));
        assertThrows(IllegalArgumentException.class,
                () -> new LogEntry(1, "   ", command, fingerprint));
    }

    @Test
    void rejectsInvalidCommandFingerprint() {
        Command command = Command.delete("alpha");

        assertThrows(NullPointerException.class,
                () -> new LogEntry(1, "request-1", command, null));
        assertThrows(IllegalArgumentException.class,
                () -> new LogEntry(1, "request-1", command, new byte[0]));
    }

    @Test
    void preservesCommand() {
        Command command = Command.put("alpha", new byte[]{7, 8, 9});

        LogEntry entry = new LogEntry(1, "request-1", command, new byte[]{1});

        assertSame(command, entry.command());
        assertEquals(command, entry.command());
    }

    @Test
    void defensivelyCopiesCommandFingerprintInput() {
        byte[] fingerprint = new byte[]{1, 2, 3};
        LogEntry entry = new LogEntry(
                1,
                "request-1",
                Command.delete("alpha"),
                fingerprint);

        fingerprint[0] = 99;

        assertArrayEquals(new byte[]{1, 2, 3}, entry.commandFingerprint());
    }

    @Test
    void commandFingerprintReturnsDefensiveCopy() {
        LogEntry entry = new LogEntry(
                1,
                "request-1",
                Command.delete("alpha"),
                new byte[]{1, 2, 3});

        byte[] returned = entry.commandFingerprint();
        returned[0] = 99;

        assertArrayEquals(new byte[]{1, 2, 3}, entry.commandFingerprint());
    }

    @Test
    void remainsImmutableThroughCommandValue() {
        byte[] originalValue = new byte[]{4, 5, 6};
        Command command = Command.put("alpha", originalValue);
        LogEntry entry = new LogEntry(1, "request-1", command, new byte[]{9});

        originalValue[0] = 100;
        byte[] returnedValue = entry.command().value().orElseThrow();
        returnedValue[1] = 100;

        assertArrayEquals(new byte[]{4, 5, 6}, entry.command().value().orElseThrow());
    }

    @Test
    void entriesWithSameContentAreEqual() {
        LogEntry first = new LogEntry(
                1,
                "request-1",
                Command.put("alpha", new byte[]{1, 2}),
                new byte[]{9, 8});
        LogEntry second = new LogEntry(
                1,
                "request-1",
                Command.put("alpha", new byte[]{1, 2}),
                new byte[]{9, 8});
        LogEntry differentFingerprint = new LogEntry(
                1,
                "request-1",
                Command.put("alpha", new byte[]{1, 2}),
                new byte[]{9, 7});

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertNotEquals(first, differentFingerprint);
        assertNotEquals(first, null);
        assertNotEquals(first, "not-a-log-entry");
    }
}
