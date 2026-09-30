package io.github.ahmadbitaarr.raftkv.wal;

import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class WalRecordCodecTest {
    private final WalRecordCodec codec = new WalRecordCodec();

    @Test
    void entryRecordRoundTrips() throws Exception {
        WalRecord original = WalRecord.entry(entry(7, "request-7", Command.put("key", new byte[]{1, 2, 3}), new byte[]{9, 8}));
        assertEquals(original, codec.decode(codec.encode(original)));
    }

    @Test
    void commitRecordRoundTrips() throws Exception {
        WalRecord original = WalRecord.commit(42);
        assertEquals(original, codec.decode(codec.encode(original)));
    }

    @Test
    void putPreservesArbitraryBinaryValue() throws Exception {
        byte[] value = new byte[]{0, -1, -128, 127, 0, 42};
        LogEntry decoded = codec.decode(codec.encode(WalRecord.entry(
                entry(1, "binary", Command.put("bin", value), new byte[]{1})))).entry();
        assertArrayEquals(value, decoded.command().value().orElseThrow());
    }

    @Test
    void deleteRoundTripsWithoutValue() throws Exception {
        LogEntry decoded = codec.decode(codec.encode(WalRecord.entry(
                entry(1, "delete", Command.delete("gone"), new byte[]{2})))).entry();
        assertEquals(Command.delete("gone"), decoded.command());
        assertTrue(decoded.command().value().isEmpty());
    }

    @Test
    void utf8KeyAndRequestIdRoundTrip() throws Exception {
        LogEntry original = entry(1, "طلب-🚀", Command.put("κλειδί-雪", new byte[]{3}), new byte[]{4});
        assertEquals(original, codec.decode(codec.encode(WalRecord.entry(original))).entry());
    }

    @Test
    void commandFingerprintRoundTripsAsBinaryData() throws Exception {
        byte[] fingerprint = new byte[]{0, -1, 5, -128, 9};
        LogEntry decoded = codec.decode(codec.encode(WalRecord.entry(
                entry(1, "fp", Command.put("k", new byte[]{1}), fingerprint)))).entry();
        assertArrayEquals(fingerprint, decoded.commandFingerprint());
    }

    @Test
    void putAllowsEmptyValue() throws Exception {
        LogEntry decoded = codec.decode(codec.encode(WalRecord.entry(
                entry(1, "empty", Command.put("k", new byte[0]), new byte[]{1})))).entry();
        assertArrayEquals(new byte[0], decoded.command().value().orElseThrow());
    }

    @Test
    void checksumDetectsModifiedPayload() throws Exception {
        byte[] encoded = codec.encode(WalRecord.entry(entry(1, "r", Command.put("k", new byte[]{1, 2}), new byte[]{3})));
        encoded[WalRecordCodec.HEADER_SIZE + 3] ^= 0x01;
        assertThrows(WalCorruptionException.class, () -> codec.decode(encoded));
    }

    @Test
    void checksumDetectsModifiedHeader() throws Exception {
        byte[] encoded = codec.encode(WalRecord.commit(0));
        encoded[4] ^= 0x01;
        assertThrows(WalCorruptionException.class, () -> codec.decode(encoded));
    }

    @Test
    void rejectsUnknownRecordType() throws Exception {
        byte[] encoded = codec.encode(WalRecord.commit(0));
        encoded[5] = 0x7F;
        rewriteChecksum(encoded);
        assertThrows(WalCorruptionException.class, () -> codec.decode(encoded));
    }

    @Test
    void rejectsUnsupportedVersion() throws Exception {
        byte[] encoded = codec.encode(WalRecord.commit(0));
        encoded[4] = 2;
        rewriteChecksum(encoded);
        assertThrows(WalCorruptionException.class, () -> codec.decode(encoded));
    }

    @Test
    void rejectsBadMagic() throws Exception {
        byte[] encoded = codec.encode(WalRecord.commit(0));
        encoded[0] = 'X';
        rewriteChecksum(encoded);
        assertThrows(WalCorruptionException.class, () -> codec.decode(encoded));
    }

    @Test
    void rejectsNegativePayloadLength() throws Exception {
        byte[] encoded = codec.encode(WalRecord.commit(0));
        ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN).putInt(6, -1);
        assertThrows(WalCorruptionException.class, () -> codec.decode(encoded));
    }

    @Test
    void rejectsOversizedPayloadLength() throws Exception {
        byte[] encoded = codec.encode(WalRecord.commit(0));
        ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN).putInt(6, WalRecordCodec.MAX_PAYLOAD_SIZE + 1);
        assertThrows(WalCorruptionException.class, () -> codec.decode(encoded));
    }

    @Test
    void rejectsMalformedEntryPayload() throws Exception {
        byte[] frame = frame(WalRecordType.ENTRY, new byte[]{0, 1, 2});
        assertThrows(WalCorruptionException.class, () -> codec.decode(frame));
    }

    @Test
    void rejectsMalformedCommitPayload() throws Exception {
        byte[] frame = frame(WalRecordType.COMMIT, new byte[]{1, 2, 3});
        assertThrows(WalCorruptionException.class, () -> codec.decode(frame));
    }

    @Test
    void rejectsMalformedUtf8() throws Exception {
        byte[] payload = validEntryPayload();
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        buffer.position(Long.BYTES);
        int requestLength = buffer.getInt();
        assertTrue(requestLength >= 2);
        int requestStart = buffer.position();
        payload[requestStart] = (byte) 0xC3;
        payload[requestStart + 1] = 0x28;
        assertThrows(WalCorruptionException.class, () -> codec.decode(frame(WalRecordType.ENTRY, payload)));
    }

    @Test
    void rejectsUnknownOperationType() throws Exception {
        byte[] payload = validEntryPayload();
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        buffer.position(Long.BYTES);
        int requestLength = buffer.getInt();
        buffer.position(buffer.position() + requestLength);
        payload[buffer.position()] = 0x7F;
        assertThrows(WalCorruptionException.class, () -> codec.decode(frame(WalRecordType.ENTRY, payload)));
    }

    @Test
    void rejectsPutWithoutValuePresence() throws Exception {
        byte[] payload = validEntryPayload();
        int valuePresenceOffset = valuePresenceOffset(payload);
        payload[valuePresenceOffset] = 0;
        assertThrows(WalCorruptionException.class, () -> codec.decode(frame(WalRecordType.ENTRY, payload)));
    }

    @Test
    void rejectsDeleteWithValuePresence() throws Exception {
        byte[] payload = entryPayload(entry(1, "delete", Command.delete("key"), new byte[]{1}));
        int valuePresenceOffset = valuePresenceOffset(payload);
        payload[valuePresenceOffset] = 1;
        assertThrows(WalCorruptionException.class, () -> codec.decode(frame(WalRecordType.ENTRY, payload)));
    }

    @Test
    void rejectsTrailingPayloadBytes() throws Exception {
        byte[] payload = validEntryPayload();
        byte[] malformed = Arrays.copyOf(payload, payload.length + 1);
        malformed[malformed.length - 1] = 1;
        assertThrows(WalCorruptionException.class, () -> codec.decode(frame(WalRecordType.ENTRY, malformed)));
    }

    @Test
    void rejectsOversizedRequestId() {
        String requestId = "x".repeat(WalRecordCodec.MAX_REQUEST_ID_BYTES + 1);
        LogEntry oversized = entry(1, requestId, Command.put("k", new byte[]{1}), new byte[]{1});
        assertThrows(IllegalArgumentException.class, () -> codec.encode(WalRecord.entry(oversized)));
    }

    @Test
    void rejectsOversizedKey() {
        String key = "k".repeat(WalRecordCodec.MAX_KEY_BYTES + 1);
        LogEntry oversized = entry(1, "r", Command.put(key, new byte[]{1}), new byte[]{1});
        assertThrows(IllegalArgumentException.class, () -> codec.encode(WalRecord.entry(oversized)));
    }

    @Test
    void rejectsOversizedValue() {
        byte[] value = new byte[WalRecordCodec.MAX_VALUE_BYTES + 1];
        LogEntry oversized = entry(1, "r", Command.put("k", value), new byte[]{1});
        assertThrows(IllegalArgumentException.class, () -> codec.encode(WalRecord.entry(oversized)));
    }

    @Test
    void rejectsOversizedFingerprint() {
        byte[] fingerprint = new byte[WalRecordCodec.MAX_FINGERPRINT_BYTES + 1];
        LogEntry oversized = entry(1, "r", Command.put("k", new byte[]{1}), fingerprint);
        assertThrows(IllegalArgumentException.class, () -> codec.encode(WalRecord.entry(oversized)));
    }

    private byte[] validEntryPayload() throws Exception {
        return entryPayload(entry(1, "request", Command.put("key", new byte[]{1, 2}), new byte[]{3, 4}));
    }

    private byte[] entryPayload(LogEntry entry) throws Exception {
        byte[] frame = codec.encode(WalRecord.entry(entry));
        int length = ByteBuffer.wrap(frame).order(ByteOrder.BIG_ENDIAN).getInt(6);
        return Arrays.copyOfRange(frame, WalRecordCodec.HEADER_SIZE, WalRecordCodec.HEADER_SIZE + length);
    }

    private static int valuePresenceOffset(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        buffer.getLong();
        int requestLength = buffer.getInt();
        buffer.position(buffer.position() + requestLength);
        buffer.get();
        int keyLength = buffer.getInt();
        buffer.position(buffer.position() + keyLength);
        return buffer.position();
    }

    private byte[] frame(WalRecordType type, byte[] payload) throws Exception {
        return WalRecordCodec.frameForTest(type, payload);
    }

    private static void rewriteChecksum(byte[] encoded) {
        WalRecordCodec.rewriteChecksumForTest(encoded);
    }

    private static LogEntry entry(long index, String requestId, Command command, byte[] fingerprint) {
        return new LogEntry(index, requestId, command, fingerprint);
    }
}
